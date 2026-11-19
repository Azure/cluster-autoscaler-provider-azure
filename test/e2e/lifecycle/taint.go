//go:build e2e

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

package lifecycle

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

// RegisterTaint adds the tainted scale-from-zero phase.
func RegisterTaint() {
	Describe("taint phase", func() {
		BeforeAll(func(ctx SpecContext) {
			Expect(utils.Env.PreparePhase(ctx, utils.TestSuiteLabelTaint)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(utils.Env.RestorePhase(ctx, utils.TestSuiteLabelTaint)).To(Succeed())
			}, NodeTimeout(30*time.Minute))
		}, NodeTimeout(75*time.Minute))
		BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
		It(
			"scales a tainted node group up from zero only for Pods that tolerate the taint",
			Label(utils.TestSuiteLabelTaint, "scale-from-zero", utils.TestSuiteLabelSlow),
			func(ctx SpecContext) {
				baseline := utils.WaitStable(ctx, 1, 0)
				Expect(utils.CheckZeroPoolTaint(baseline, utils.Env.Config)).To(Succeed())

				By("creating zero-pool demand without the toleration")
				blocked := utils.Env.Deployment(utils.Namespace.Name, "blocked-taint", utils.ZeroLabel, "50m", 1)
				Expect(utils.Env.K8s.Create(ctx, blocked)).To(Succeed())
				pods := utils.WaitWorkload(ctx, blocked.Name, utils.Env.Config.ZeroPool, 0, 1)

				By("checking that the zero pool does not grow for it")
				utils.WaitPodEvent(ctx, pods[0], "NotTriggerScaleUp")

				utils.ObserveNoGrowth(ctx, blocked.Name, utils.Env.Config.ZeroPool, 0, 1, 5*time.Minute)

				By("creating zero-pool demand with the toleration and waiting for growth")
				tolerated := utils.Env.Deployment(utils.Namespace.Name, "tolerated-taint", utils.ZeroLabel, "50m", 1)
				tolerated.Spec.Template.Spec.Tolerations = append(tolerated.Spec.Template.Spec.Tolerations, corev1.Toleration{
					Key:      utils.RunLabel,
					Operator: corev1.TolerationOpEqual,
					Value:    utils.Env.Config.RunID,
					Effect:   corev1.TaintEffectNoSchedule,
				})
				Expect(utils.Env.K8s.Create(ctx, tolerated)).To(Succeed())
				grown := utils.WaitStable(ctx, 1, 1)
				Expect(utils.CheckZeroPoolTaint(grown, utils.Env.Config)).To(Succeed())
				Expect(utils.CheckZeroNodeTaint(grown, utils.Env.Config)).To(Succeed())
				utils.WaitWorkload(ctx, tolerated.Name, utils.Env.Config.ZeroPool, 1, 0)
				utils.WaitWorkload(ctx, blocked.Name, utils.Env.Config.ZeroPool, 0, 1)

				By("removing the demand and waiting for the zero-pool worker to be deleted")
				Expect(utils.Env.K8s.Delete(ctx, blocked)).To(Succeed())
				Expect(utils.Env.K8s.Delete(ctx, tolerated)).To(Succeed())
				utils.WaitDeleted(ctx, grown, utils.Env.Config.ZeroPool, 1, 1, 0)
			},
			NodeTimeout(45*time.Minute),
		)
	})
}
