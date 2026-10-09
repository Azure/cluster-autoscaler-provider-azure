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
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const (
	suspendedCondition          apiv1.NodeConditionType = "Suspended"
	suspendedTaintKey                                   = taints.StatusTaintPrefix + "azure-suspended"
	scaleDownDisabledAnnotation                         = "cluster-autoscaler.kubernetes.io/scale-down-disabled"
	parkedReason                                        = "AzureVMDeallocated"
	resumedReason                                       = "AzureVMStarted"
)

type suspendedParkOperation struct {
	node   *apiv1.Node
	vmID   string
	poller *runtime.Poller[armcompute.VirtualMachineScaleSetVMsClientDeallocateResponse]
}

func (s *ScaleSet) deleteSuspendedNodes(ctx context.Context, nodes []*apiv1.Node, enforceMinimum bool) error {
	if s.manager.kubeClient == nil {
		return fmt.Errorf("suspended mode requires a Kubernetes client")
	}

	ctx, cancel := context.WithTimeout(ctx, asyncContextTimeout)
	defer cancel()
	nodeList, err := s.manager.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list nodes before scale-down: %w", err)
	}

	registered := make(map[string]*apiv1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		registered[normalizeProviderID(node.Spec.ProviderID)] = node
	}

	parkNodes := make([]*apiv1.Node, 0, len(nodes))
	syntheticNodes := make([]*apiv1.Node, 0, len(nodes))
	var failedResumes map[string]suspendedPowerOperation
	for _, node := range nodes {
		reason := node.Annotations[cloudprovider.FakeNodeReasonAnnotation]
		isSynthetic := node.UID == "" &&
			(reason == cloudprovider.FakeNodeUnregistered || reason == cloudprovider.FakeNodeCreateError)
		current := registered[normalizeProviderID(node.Spec.ProviderID)]
		if isSynthetic {
			if current == nil {
				syntheticNodes = append(syntheticNodes, node)
				continue
			}

			if reason != cloudprovider.FakeNodeCreateError {
				return fmt.Errorf("refusing synthetic cleanup of registered instance %s", node.Spec.ProviderID)
			}

			if failedResumes == nil {
				vms, err := s.suspendedInventory(ctx, true)
				if err != nil {
					return err
				}
				failedResumes = make(map[string]suspendedPowerOperation)
				for _, vm := range vms {
					op, pending := s.powerOperation(*vm.Properties.VMID)
					if pending && !op.park && op.failed && !op.polling && op.node != nil {
						failedResumes[normalizeProviderID(azurePrefix+*vm.ID)] = op
					}
				}
			}
			op, failed := failedResumes[normalizeProviderID(node.Spec.ProviderID)]
			if !failed || sameSuspendedNode(current, op.node) != nil {
				return fmt.Errorf("refusing synthetic cleanup of registered instance %s", node.Spec.ProviderID)
			}
			parkNodes = append(parkNodes, current)
		} else {
			if current == nil || current.Name != node.Name || current.UID != node.UID || node.UID == "" {
				return fmt.Errorf("node %s changed identity or is not registered", node.Name)
			}
			parkNodes = append(parkNodes, node)
		}
	}

	if len(parkNodes) > 0 {
		if err := s.parkSuspendedNodes(ctx, parkNodes, enforceMinimum); err != nil {
			return err
		}
	}

	if len(syntheticNodes) > 0 {
		if err := s.lockParkOperation(ctx); err != nil {
			return err
		}

		defer s.parkMutex.Unlock()
		if enforceMinimum {
			size, err := s.getScaleSetSize()
			if err != nil {
				return err
			}
			if int(size) <= s.MinSize(ctx) {
				return fmt.Errorf("min size reached, nodes will not be deleted")
			}
		}

		currentNodes, err := s.manager.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("recheck nodes before synthetic cleanup: %w", err)
		}

		for _, current := range currentNodes.Items {
			for _, node := range syntheticNodes {
				if normalizeProviderID(current.Spec.ProviderID) == normalizeProviderID(node.Spec.ProviderID) {
					return fmt.Errorf("refusing synthetic cleanup of registered instance %s", node.Spec.ProviderID)
				}
			}
		}
		return s.forceDeleteNodes(ctx, syntheticNodes)
	}

	return nil
}

func (s *ScaleSet) parkSuspendedNodes(ctx context.Context, nodes []*apiv1.Node, enforceMinimum bool) error {
	if s.manager.kubeClient == nil || s.manager.azClient.vmssPowerClient == nil {
		return fmt.Errorf("suspended mode requires Kubernetes and VMSS power clients")
	}

	accepted, err := s.submitSuspendedParking(ctx, nodes, enforceMinimum)
	for _, operation := range accepted {
		if finishErr := s.finishSuspendedParking(ctx, operation); finishErr != nil {
			err = errors.Join(err, finishErr)
		}
	}
	return err
}

func (s *ScaleSet) submitSuspendedParking(ctx context.Context, nodes []*apiv1.Node, enforceMinimum bool) ([]suspendedParkOperation, error) {
	if err := s.lockParkOperation(ctx); err != nil {
		return nil, err
	}
	defer s.parkMutex.Unlock()
	vms, err := s.suspendedInventory(ctx, true)
	if err != nil {
		return nil, err
	}

	if err := s.reconcileSuspendedPower(ctx, vms); err != nil {
		return nil, err
	}

	byProviderID := make(map[string]*armcompute.VirtualMachineScaleSetVM, len(vms))
	for _, vm := range vms {
		byProviderID[normalizeProviderID(azurePrefix+*vm.ID)] = vm
	}

	var newParks int
	for _, node := range nodes {
		vm := byProviderID[normalizeProviderID(node.Spec.ProviderID)]
		if vm == nil {
			return nil, fmt.Errorf("node %s is not a VM in %s", node.Name, s.Name)
		}
		if err := s.checkNodeIdentity(ctx, node); err != nil {
			return nil, err
		}

		op, pending := s.powerOperation(*vm.Properties.VMID)
		completedResume := !op.park && op.completed && isAuthoritativelyParked(vm)
		if pending && (op.polling || !op.failed && !completedResume) {
			return nil, fmt.Errorf("power operation pending for %s", node.Name)
		}
		state := suspendedPowerState(vm)
		parked := s.effectivelyParked(vm) || pending && op.failed && isAuthoritativelyParked(vm)
		stablePower := state == vmPowerStateRunning || state == vmPowerStateStopped || state == vmPowerStateDeallocated
		failedResume := pending && !op.park && op.failed
		if !parked && !stablePower && !failedResume {
			return nil, fmt.Errorf("cannot park node %s with power state %s", node.Name, state)
		}
		if !s.effectivelyParked(vm) {
			newParks++
		}
	}

	if enforceMinimum && newParks > 0 {
		total, err := s.getScaleSetSize()
		if err != nil {
			return nil, err
		}

		running := int(total) - s.parkedCount()
		if running-newParks < s.minSize {
			return nil, fmt.Errorf("min size reached: parking %d nodes leaves %d running, minimum %d",
				newParks, running-newParks, s.minSize)
		}
	}

	accepted := make([]suspendedParkOperation, 0, len(nodes))
	for _, node := range nodes {
		vm := byProviderID[normalizeProviderID(node.Spec.ProviderID)]
		op := suspendedParkOperation{node: node.DeepCopy(), vmID: *vm.Properties.VMID}
		parked := s.effectivelyParked(vm)
		if previous, pending := s.powerOperation(op.vmID); pending &&
			(previous.failed || previous.completed) && isAuthoritativelyParked(vm) {
			parked = true
		}
		if !parked {
			submitCtx, cancel := context.WithTimeout(ctx, vmssContextTimeout)
			poller, err := s.manager.azClient.vmssPowerClient.BeginDeallocate(
				submitCtx, s.manager.config.ResourceGroup, s.Name, *vm.InstanceID, nil)
			cancel()
			if err != nil && isDefinitePowerRejection(err) {
				return accepted, fmt.Errorf("deallocate %s: %w", node.Name, err)
			}

			s.recordPower(op.vmID, suspendedPowerOperation{node: op.node, park: true, polling: poller != nil})
			if poller != nil {
				op.poller = poller
				accepted = append(accepted, op)
			}
			if err != nil || poller == nil {
				if err == nil {
					err = fmt.Errorf("no operation returned")
				}
				return accepted, fmt.Errorf("deallocate %s has uncertain acceptance: %w", node.Name, err)
			}

			continue
		}

		s.recordPower(op.vmID, suspendedPowerOperation{node: op.node, park: true, completed: true})
		accepted = append(accepted, op)
	}

	return accepted, nil
}

func (s *ScaleSet) finishSuspendedParking(ctx context.Context, operation suspendedParkOperation) error {
	if operation.poller != nil {
		_, err := operation.poller.PollUntilDone(ctx, nil)
		s.finishPowerPolling(operation.vmID, err)
		s.invalidateSuspendedInventory()
		if err != nil {
			return fmt.Errorf("waiting for deallocate %s: %w", operation.node.Name, err)
		}
	}

	if err := s.lockParkOperation(ctx); err != nil {
		return err
	}
	defer s.parkMutex.Unlock()
	vms, err := s.suspendedInventory(ctx, true)
	if err != nil {
		return err
	}

	for _, vm := range vms {
		if *vm.Properties.VMID == operation.vmID {
			return s.reconcileSuspendedPower(ctx, []*armcompute.VirtualMachineScaleSetVM{vm})
		}
	}
	return fmt.Errorf("VM for node %s changed identity during deallocation", operation.node.Name)
}

// Target counts retained VMs; adding parked VMs to the configured running bounds
// makes MaxSize-TargetSize and MinSize-TargetSize use running capacity.
// Resume changes no target capacity and immediately removes a VM from the parked count.
// Suspended=False's transition time lets the core count an old, unready Node as upcoming.
func (s *ScaleSet) increaseSuspended(ctx context.Context, delta int) error {
	if delta <= 0 {
		return fmt.Errorf("size increase must be positive")
	}

	ctx, cancel := context.WithTimeout(ctx, vmssContextTimeout)
	defer cancel()
	if s.manager.kubeClient == nil || s.manager.azClient.vmssPowerClient == nil {
		return fmt.Errorf("suspended mode requires Kubernetes and VMSS power clients")
	}
	if err := s.lockParkOperation(ctx); err != nil {
		return err
	}
	accepted := make([]suspendedStartOperation, 0, delta)
	defer func() {
		s.parkMutex.Unlock()
		for _, operation := range accepted {
			go s.waitForSuspendedStart(operation)
		}
	}()
	vms, err := s.suspendedInventory(ctx, true)
	if err != nil {
		return err
	}

	if err := s.reconcileSuspendedPower(ctx, vms); err != nil {
		return err
	}
	if _, err := s.canIncreaseSize(delta); err != nil {
		return err
	}

	nodeList, err := s.manager.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list nodes before scale-up: %w", err)
	}

	nodes := make(map[string]*apiv1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		nodes[normalizeProviderID(node.Spec.ProviderID)] = node
	}

	var rejections error
	for _, vm := range vms {
		if delta == 0 || !s.effectivelyParked(vm) {
			continue
		}
		if _, pending := s.powerOperation(*vm.Properties.VMID); pending {
			continue
		}
		node := nodes[normalizeProviderID(azurePrefix+*vm.ID)]
		if node != nil {
			if err := s.checkNodeIdentity(ctx, node); err != nil {
				return err
			}
			node = node.DeepCopy()
		}

		// Keep suspension metadata untouched until Azure accepts Start.
		poller, err := s.manager.azClient.vmssPowerClient.BeginStart(
			ctx, s.manager.config.ResourceGroup, s.Name, *vm.InstanceID, nil)
		if err != nil && isDefinitePowerRejection(err) {
			rejection := fmt.Errorf("start %s: %w", *vm.ID, err)
			klog.Warning(rejection)
			rejections = errors.Join(rejections, rejection)
			continue
		}

		s.recordPower(*vm.Properties.VMID, suspendedPowerOperation{node: node, polling: poller != nil})
		if poller != nil {
			accepted = append(accepted, suspendedStartOperation{
				poller: poller, vmID: *vm.Properties.VMID, resourceID: *vm.ID,
			})
		}
		if node != nil {
			if updateErr := s.setNodeSuspended(ctx, node, false); updateErr != nil {
				return errors.Join(err, updateErr)
			}
		}

		if err != nil || poller == nil {
			if err == nil {
				err = fmt.Errorf("no operation returned")
			}
			return fmt.Errorf("start %s has uncertain acceptance: %w", *vm.ID, err)
		}

		delta--
	}

	if delta == 0 {
		return nil
	}

	for _, vm := range vms {
		op, pending := s.powerOperation(*vm.Properties.VMID)
		transitional := suspendedPowerState(vm) == vmPowerStateDeallocating ||
			suspendedPowerState(vm) == vmPowerStateStopping
		if pending && op.park || transitional {
			return errors.Join(rejections,
				fmt.Errorf("parking is still in progress for %s; refusing replacement growth", *vm.ID))
		}
	}

	physical, err := s.getScaleSetSize()
	if err != nil {
		return errors.Join(rejections, err)
	}
	if err := s.setScaleSetSize(physical+int64(delta), delta); err != nil {
		return errors.Join(rejections, err)
	}

	return nil
}

func (s *ScaleSet) checkNodeIdentity(ctx context.Context, node *apiv1.Node) error {
	current, err := s.manager.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	return sameSuspendedNode(current, node)
}

func (s *ScaleSet) reconcileSettledSuspendedNodes(
	ctx context.Context, vms []*armcompute.VirtualMachineScaleSetVM, nodeList *apiv1.NodeList,
) error {
	byProviderID := make(map[string]*armcompute.VirtualMachineScaleSetVM, len(vms))
	for _, vm := range vms {
		byProviderID[normalizeProviderID(azurePrefix+*vm.ID)] = vm
	}

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		vm := byProviderID[normalizeProviderID(node.Spec.ProviderID)]
		if vm == nil {
			continue
		}
		s.powerMutex.Lock()
		_, pending := s.pendingPower[*vm.Properties.VMID]
		_, overridden := s.powerOverrides[*vm.Properties.VMID]
		s.powerMutex.Unlock()
		if pending || overridden {
			continue
		}

		if ptr.Deref(vm.Properties.ProvisioningState, "") != provisioningStateSucceeded {
			continue
		}

		parked := isAuthoritativelyParked(vm)
		if parked && hasSuspendedMetadata(node) {
			continue
		}

		if !parked {
			if suspendedPowerState(vm) != vmPowerStateRunning {
				continue
			}
			suspended := taints.HasTaint(node, suspendedTaintKey)
			for _, condition := range node.Status.Conditions {
				if condition.Type == suspendedCondition && condition.Status == apiv1.ConditionTrue {
					suspended = true
				}
			}

			if !suspended {
				continue
			}
		}

		if err := s.setNodeSuspended(ctx, node, parked); err != nil {
			return err
		}
	}

	return nil
}

func hasSuspendedMetadata(node *apiv1.Node) bool {
	if node.Annotations[scaleDownDisabledAnnotation] != "true" {
		return false
	}

	suspended := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == suspendedCondition {
			suspended = condition.Status == apiv1.ConditionTrue
			break
		}
	}

	if !suspended {
		return false
	}

	suspendedTaints := 0
	for _, taint := range node.Spec.Taints {
		switch taint.Key {
		case taints.ToBeDeletedTaint, taints.DeletionCandidateTaintKey:
			return false
		case suspendedTaintKey:
			if taint != (apiv1.Taint{Key: suspendedTaintKey, Effect: apiv1.TaintEffectNoSchedule}) {
				return false
			}
			suspendedTaints++
		}
	}
	return suspendedTaints == 1
}

func sameSuspendedNode(current, expected *apiv1.Node) error {
	if expected.UID == "" || current.UID != expected.UID ||
		normalizeProviderID(current.Spec.ProviderID) != normalizeProviderID(expected.Spec.ProviderID) {
		return fmt.Errorf("node %s changed identity", expected.Name)
	}
	return nil
}

func (s *ScaleSet) setNodeSuspended(ctx context.Context, node *apiv1.Node, suspended bool) error {
	client := s.manager.kubeClient.CoreV1().Nodes()
	status, reason := apiv1.ConditionFalse, resumedReason
	if suspended {
		status, reason = apiv1.ConditionTrue, parkedReason
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if err := sameSuspendedNode(current, node); err != nil {
			return err
		}
		current = current.DeepCopy()
		if !setSuspendedCondition(current, status, reason, metav1.Now()) {
			return nil
		}

		_, err = client.UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("set %s=%s on node %s: %w", suspendedCondition, status, node.Name, err)
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if err := sameSuspendedNode(current, node); err != nil {
			return err
		}

		updated := current.DeepCopy()
		coreCordon := taints.HasToBeDeletedTaint(current) && s.manager.cordonNodeBeforeTerminate
		kept := make([]apiv1.Taint, 0, len(updated.Spec.Taints)+1)
		for _, taint := range updated.Spec.Taints {
			coreDeletion := taint.Key == taints.ToBeDeletedTaint || taint.Key == taints.DeletionCandidateTaintKey
			if taint.Key == suspendedTaintKey || suspended && coreDeletion {
				continue
			}
			kept = append(kept, taint)
		}
		if suspended {
			if updated.Annotations == nil {
				updated.Annotations = make(map[string]string)
			}

			updated.Annotations[scaleDownDisabledAnnotation] = "true"
			kept = append(kept, apiv1.Taint{Key: suspendedTaintKey, Effect: apiv1.TaintEffectNoSchedule})
			if coreCordon {
				updated.Spec.Unschedulable = false
			}
		} else {
			delete(updated.Annotations, scaleDownDisabledAnnotation)
		}

		updated.Spec.Taints = kept
		if equality.Semantic.DeepEqual(current, updated) {
			return nil
		}

		_, err = client.Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("update suspended metadata on node %s: %w", node.Name, err)
	}

	return nil
}

func setSuspendedCondition(node *apiv1.Node, status apiv1.ConditionStatus, reason string, now metav1.Time) bool {
	condition := apiv1.NodeCondition{
		Type: suspendedCondition, Status: status, Reason: reason,
		Message:           "Set by the Azure cluster autoscaler provider",
		LastHeartbeatTime: now, LastTransitionTime: now,
	}

	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type != suspendedCondition {
			continue
		}

		if node.Status.Conditions[i].Status == status {
			return false
		}

		node.Status.Conditions[i] = condition
		return true
	}

	node.Status.Conditions = append(node.Status.Conditions, condition)
	return true
}
