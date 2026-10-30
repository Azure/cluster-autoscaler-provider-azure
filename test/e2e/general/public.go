//go:build e2e

/*
Copyright The Kubernetes Authors.

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

package general

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

var _ = Describe("Azure Provider", func() {
	BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
	It(
		"does not scale up for a Pod larger than every node",
		Label("memory", "Feature:ClusterSizeAutoscalingScaleUp"),
		func(ctx SpecContext) {
			By("creating a Pod that requests more memory than a worker has")
			memory := utils.MemoryGeometry(ctx, 1.1, false)
			workload := utils.MemoryDeployment("oversized", utils.MainLabel, 1, memory)
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())
			pods := utils.WaitWorkload(ctx, workload.Name, utils.Env.Config.MainPool, 0, 1)

			By("waiting for the NotTriggerScaleUp event and checking that no worker is added")
			utils.WaitPodEvent(ctx, pods[0], "NotTriggerScaleUp")
			utils.ObserveNoGrowth(ctx, workload.Name, utils.Env.Config.MainPool, 0, 1)
		},
		NodeTimeout(25*time.Minute),
	)

	It(
		"scales up to run 100 small memory-requesting Pods",
		Label("memory", "Feature:ClusterSizeAutoscalingScaleUp", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			By("creating 100 small memory-requesting Pods")
			memory := utils.MemoryGeometry(ctx, 1, false)
			workload := utils.MemoryDeployment("small", utils.MainLabel, 100, (memory+99)/100)
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())

			By("waiting for main to grow and run every Pod")
			grown := utils.WaitStable(ctx, 2, 0)
			utils.WaitWorkload(ctx, workload.Name, utils.Env.Config.MainPool, 100, 0)

			By("removing the demand and waiting for the added worker to be deleted")
			Expect(utils.Env.K8s.Delete(ctx, workload)).To(Succeed())
			utils.WaitBaselineDeleted(ctx, grown, 0, nil)
		},
		NodeTimeout(50*time.Minute),
	)

	It(
		"scales up for a host port conflict",
		Label("host-port", "Feature:ClusterSizeAutoscalingScaleUp", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			workload := utils.Env.Deployment(utils.Namespace.Name, "host-port", "", "10m", 3)
			workload.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{
				ContainerPort: 4321,
				HostPort:      4321,
				Protocol:      corev1.ProtocolTCP,
			}}
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())
			utils.WaitWorkers(ctx, 3)
			utils.AssertDistinctNodes(utils.WaitWorkload(ctx, workload.Name, "", 3, 0), 3)
		},
		NodeTimeout(35*time.Minute),
	)

	It(
		"scales up for hostname anti-affinity",
		Label("anti-affinity", "Feature:ClusterSizeAutoscalingScaleUp", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			By("starting one anti-affinity Pod")
			workload := utils.AntiAffinityDeployment("anti-affinity", "", 1)
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())
			utils.WaitWorkload(ctx, workload.Name, "", 1, 0)

			By("scaling to three Pods and waiting for three workers")
			utils.ScaleDeployment(ctx, workload, 3)
			utils.WaitWorkers(ctx, 3)
			utils.AssertDistinctNodes(utils.WaitWorkload(ctx, workload.Name, "", 3, 0), 3)
		},
		NodeTimeout(35*time.Minute),
	)

	It(
		"scales up for a pending Pod with an EmptyDir volume",
		Label("emptydir", "Feature:ClusterSizeAutoscalingScaleUp", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			By("starting one EmptyDir Pod")
			workload := utils.AntiAffinityDeployment("empty-dir", utils.MainLabel, 1)
			workload.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
			workload.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "scratch", MountPath: "/scratch"}}
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())
			utils.WaitWorkload(ctx, workload.Name, utils.Env.Config.MainPool, 1, 0)

			By("scaling to two Pods and waiting for main to grow")
			utils.ScaleDeployment(ctx, workload, 2)
			utils.WaitStable(ctx, 2, 0)
			utils.AssertDistinctNodes(utils.WaitWorkload(ctx, workload.Name, utils.Env.Config.MainPool, 2, 0), 2)
		},
		NodeTimeout(35*time.Minute),
	)

	It(
		"deletes the added nodes after the demand goes away",
		Label("scale-down", "Feature:ClusterSizeAutoscalingScaleDown", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			workload, grown := utils.GrowThree(ctx)

			By("removing the demand and waiting for the added workers to be deleted")
			Expect(utils.Env.K8s.Delete(ctx, workload)).To(Succeed())
			utils.WaitBaselineDeleted(ctx, grown, 0, nil)
		},
		NodeTimeout(50*time.Minute),
	)

	DescribeTable(
		"honors a PodDisruptionBudget during scale-down",
		func(ctx SpecContext, podsPerNode, allowed int) {
			utils.Drain(ctx, podsPerNode, allowed, utils.Namespace.Name)
		},
		Entry(
			"when one disruption is allowed",
			Label("drain", "Feature:ClusterSizeAutoscalingScaleDown", utils.TestSuiteLabelSlow),
			NodeTimeout(55*time.Minute),
			1,
			1,
		),
		Entry(
			"when no disruption is allowed",
			Label("drain", "Feature:ClusterSizeAutoscalingScaleDown", utils.TestSuiteLabelSlow),
			NodeTimeout(55*time.Minute),
			1,
			0,
		),
		Entry(
			"when two Pods per node share one disruption",
			Label("drain", "Feature:ClusterSizeAutoscalingScaleDown", utils.TestSuiteLabelSlow),
			NodeTimeout(55*time.Minute),
			2,
			1,
		),
	)
})
