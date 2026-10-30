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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

var _ = Describe("Azure Provider", Label("priority"), func() {
	BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
	var memory int64
	var low, high string

	BeforeEach(func(ctx SpecContext) {
		low, high = utils.PriorityClasses(ctx)
		memory = utils.MemoryGeometry(ctx, 0.7, true)
	})

	It(
		"does not scale up for a pending expendable Pod",
		Label("Feature:ClusterSizeAutoscalingScaleUp", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			By("creating two expendable Pods that need two workers")
			workload := utils.PriorityDeployment("expendable", utils.MainLabel, low, 2, memory)
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())
			utils.WaitWorkload(ctx, workload.Name, utils.Env.Config.MainPool, 1, 1)

			By("checking that main does not grow for the pending expendable Pod")
			utils.ObserveNoGrowth(ctx, workload.Name, utils.Env.Config.MainPool, 1, 1, 5*time.Minute)
		},
		NodeTimeout(30*time.Minute),
	)

	It(
		"scales up for pending non-expendable Pods",
		Label("Feature:ClusterSizeAutoscalingScaleUp", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			By("creating two high-priority Pods that need two workers")
			workload := utils.PriorityDeployment("high", utils.MainLabel, high, 2, memory)
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())

			By("waiting for main to grow and run both Pods")
			utils.WaitStable(ctx, 2, 0)
			utils.AssertDistinctNodes(utils.WaitWorkload(ctx, workload.Name, utils.Env.Config.MainPool, 2, 0), 2)
		},
		NodeTimeout(35*time.Minute),
	)

	It(
		"preempts expendable Pods and doesn't scale up for their replacements",
		Label("Feature:ClusterSizeAutoscalingScaleUp", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			By("running one expendable Pod")
			expendable := utils.PriorityDeployment("low", utils.MainLabel, low, 1, memory)
			Expect(utils.Env.K8s.Create(ctx, expendable)).To(Succeed())
			old := utils.WaitWorkload(ctx, expendable.Name, utils.Env.Config.MainPool, 1, 0)[0]

			By("creating a high-priority Pod that preempts it")
			important := utils.PriorityDeployment("high", utils.MainLabel, high, 1, memory)
			Expect(utils.Env.K8s.Create(ctx, important)).To(Succeed())
			utils.WaitWorkload(ctx, important.Name, utils.Env.Config.MainPool, 1, 0)

			Eventually(
				ctx,
				func() error {
					return utils.Env.K8s.Get(ctx, client.ObjectKeyFromObject(&old), &corev1.Pod{})
				},
				2*time.Minute,
				utils.PollInterval,
			).Should(MatchError(apierrors.IsNotFound, "IsNotFound"),
				"the original low-priority Pod %s must actually be preempted", old.Name)

			By("checking that main does not grow for the replacement expendable Pod")
			utils.WaitWorkload(ctx, expendable.Name, utils.Env.Config.MainPool, 0, 1)
			utils.ObserveNoGrowth(ctx, expendable.Name, utils.Env.Config.MainPool, 0, 1, 5*time.Minute)
			utils.WaitWorkload(ctx, important.Name, utils.Env.Config.MainPool, 1, 0)
		},
		NodeTimeout(35*time.Minute),
	)

	It(
		"deletes nodes that run only expendable Pods",
		Label("Feature:ClusterSizeAutoscalingScaleDown", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			growth, grown := utils.GrowThree(ctx)

			By("running one expendable reservation on each worker")
			workload := utils.PriorityDeployment("expendable", "", low, 3, memory)
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())
			utils.AssertDistinctNodes(utils.WaitWorkload(ctx, workload.Name, "", 3, 0), 3)

			By("removing the growth demand and waiting for the added workers to be deleted")
			Expect(utils.Env.K8s.Delete(ctx, growth)).To(Succeed())
			utils.WaitBaselineDeleted(ctx, grown, 0, nil)
			utils.WaitWorkload(ctx, workload.Name, utils.Env.Config.MainPool, 1, 2)
		},
		NodeTimeout(50*time.Minute),
	)

	It(
		"keeps nodes that run non-expendable Pods",
		Label("Feature:ClusterSizeAutoscalingScaleDown", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			growth, grown := utils.GrowThree(ctx)

			By("running one high-priority reservation on each worker")
			workload := utils.PriorityDeployment("important", "", high, 3, memory)
			Expect(utils.Env.K8s.Create(ctx, workload)).To(Succeed())
			utils.AssertDistinctNodes(utils.WaitWorkload(ctx, workload.Name, "", 3, 0), 3)

			By("removing the growth demand and checking that every worker stays")
			Expect(utils.Env.K8s.Delete(ctx, growth)).To(Succeed())

			utils.ObserveKept(ctx, grown, utils.Namespace.Name, workload.Name, 3)
		},
		NodeTimeout(40*time.Minute),
	)
})
