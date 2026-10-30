/*
Copyright (c) Microsoft Corporation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package azure

import (
	"context"
	"slices"
	"time"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	cloudproviderapi "k8s.io/cloud-provider/api"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	caerrors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
)

// resumeTaints keep pods off a Node only while its VM boots.
var resumeTaints = map[string]bool{
	suspendedTaintKey:                  true,
	cloudproviderapi.TaintNodeShutdown: true,
	apiv1.TaintNodeNotReady:            true,
	apiv1.TaintNodeUnreachable:         true,
	apiv1.TaintNodeOutOfService:        true,
}

type resumeFailure struct {
	group     cloudprovider.NodeGroup
	errorInfo cloudprovider.InstanceErrorInfo
}

func (m *AzureManager) queueResumeFailure(group cloudprovider.NodeGroup, errorInfo cloudprovider.InstanceErrorInfo) {
	m.resumeFailureMutex.Lock()
	defer m.resumeFailureMutex.Unlock()
	m.resumeFailures = append(m.resumeFailures, resumeFailure{group: group, errorInfo: errorInfo})
}

// reportResumeFailures backs off groups with failed resumes. It runs in the pod list processor,
// after core builds the templates its backoff needs and before scale-up.
func (m *AzureManager) reportResumeFailures(ctx context.Context) {
	m.resumeFailureMutex.Lock()
	failures := m.resumeFailures
	m.resumeFailures = nil
	m.resumeFailureMutex.Unlock()
	for _, failure := range failures {
		m.scaleStateNotifier.RegisterFailedScaleUp(ctx, failure.group, 1, failure.errorInfo, time.Now())
	}
}

// registerResumeProcessors lets in-flight resumes take pending pods in simulation while
// keeping them out of scale-down. Core keeps their Nodes in the Suspended bucket.
func (m *AzureManager) registerResumeProcessors(processors *ca_processors.AutoscalingProcessors) {
	processors.PodListProcessor = &resumePodListProcessor{PodListProcessor: processors.PodListProcessor, manager: m}
	processors.ScaleDownNodeProcessor = &resumeScaleDownNodeProcessor{
		ScaleDownNodeProcessor: processors.ScaleDownNodeProcessor, manager: m,
	}
	m.scaleStateNotifier = processors.ScaleStateNotifier
}

// resumingNodes returns the UIDs of Nodes with in-flight resumes by Node name.
func (m *AzureManager) resumingNodes() map[string]types.UID {
	return m.nodesWithPower(func(op suspendedPowerOperation) bool { return !op.park && !op.failed })
}

// nodesWithPower returns the UIDs, by Node name, of Nodes whose pending power operation matches.
func (m *AzureManager) nodesWithPower(match func(suspendedPowerOperation) bool) map[string]types.UID {
	nodes := make(map[string]types.UID)
	for _, group := range m.getNodeGroups() {
		s, ok := group.(*ScaleSet)
		if !ok || !s.suspendsParkedNodes() {
			continue
		}

		s.powerMutex.Lock()
		for _, op := range s.pendingPower {
			if match(op) {
				nodes[op.node.Name] = op.node.UID
			}
		}
		s.powerMutex.Unlock()
	}
	return nodes
}

type resumePodListProcessor struct {
	pods.PodListProcessor
	manager *AzureManager
}

// Process makes each in-flight resume schedulable in the cluster snapshot before the core
// processors run, so pending pods pack onto the real Node instead of triggering a scale-up.
func (p *resumePodListProcessor) Process(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, unschedulablePods []*apiv1.Pod) ([]*apiv1.Pod, error) {
	p.manager.reportResumeFailures(ctx)
	snapshot := autoscalingCtx.ClusterSnapshot
	for name, uid := range p.manager.resumingNodes() {
		nodeInfo, err := snapshot.GetNodeInfo(name)
		// The snapshot may not list the Node yet, or may still list a replaced one.
		if err != nil || nodeInfo.Node().UID != uid {
			continue
		}

		if err := snapshot.RemoveNodeInfo(ctx, name); err != nil {
			return nil, err
		}
		resumed := framework.NewNodeInfo(schedulableNode(nodeInfo.Node()), nodeInfo.LocalResourceSlices, nodeInfo.Pods()...)
		if nodeInfo.CSINode != nil {
			resumed.SetCSINode(nodeInfo.CSINode)
		}
		if err := snapshot.AddNodeInfo(resumed); err != nil {
			return nil, err
		}
	}
	return p.PodListProcessor.Process(ctx, autoscalingCtx, unschedulablePods)
}

func schedulableNode(node *apiv1.Node) *apiv1.Node {
	node = node.DeepCopy()
	node.Spec.Taints = slices.DeleteFunc(node.Spec.Taints, func(taint apiv1.Taint) bool {
		return resumeTaints[taint.Key]
	})
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == apiv1.NodeReady {
			node.Status.Conditions[i].Status = apiv1.ConditionTrue
		}
	}
	return node
}

type resumeScaleDownNodeProcessor struct {
	nodes.ScaleDownNodeProcessor
	manager *AzureManager
}

// GetPodDestinationCandidates keeps pods from being simulated onto in-flight resumes.
func (p *resumeScaleDownNodeProcessor) GetPodDestinationCandidates(autoscalingCtx *ca_context.AutoscalingContext, allNodes []*apiv1.Node) ([]*apiv1.Node, caerrors.AutoscalerError) {
	return p.ScaleDownNodeProcessor.GetPodDestinationCandidates(autoscalingCtx, p.withoutResumes(allNodes))
}

// GetScaleDownCandidates keeps in-flight resumes from being scaled down.
func (p *resumeScaleDownNodeProcessor) GetScaleDownCandidates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, allNodes []*apiv1.Node) ([]*apiv1.Node, caerrors.AutoscalerError) {
	return p.ScaleDownNodeProcessor.GetScaleDownCandidates(ctx, autoscalingCtx, p.withoutResumes(allNodes))
}

// withoutResumes also drops failed resumes and re-parks: a resume can fail after this loop's snapshot
// already made its Node schedulable.
func (p *resumeScaleDownNodeProcessor) withoutResumes(allNodes []*apiv1.Node) []*apiv1.Node {
	resuming := p.manager.nodesWithPower(func(op suspendedPowerOperation) bool { return !op.resumed.IsZero() })
	return slices.DeleteFunc(slices.Clone(allNodes), func(node *apiv1.Node) bool {
		_, found := resuming[node.Name]
		return found
	})
}
