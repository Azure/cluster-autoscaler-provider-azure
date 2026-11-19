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

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

// RegisterBalance adds the similar-pool balancing phase.
func RegisterBalance() {
	Describe("balance phase", func() {
		BeforeAll(func(ctx SpecContext) {
			Expect(utils.Env.PreparePhase(ctx, utils.TestSuiteLabelBalance)).To(Succeed())
		}, NodeTimeout(75*time.Minute))
		BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
		It(
			"splits one two-node scale-up across similar pools",
			Label(utils.TestSuiteLabelBalance, utils.TestSuiteLabelSlow),
			func(ctx SpecContext) {
				By("creating pending demand for the similar pools")
				c := utils.Env.Config
				Expect(utils.ControllerReplicas(ctx)).To(Equal(int32(0)))
				before := utils.WaitStable(ctx, 1, 0)
				Expect(before.Pools[c.BalancePoolA].Max).To(Equal(2))
				Expect(before.Pools[c.BalancePoolB].Max).To(Equal(2))
				work := utils.AntiAffinityDeployment("balance-demand", utils.BalanceLabel, 2)
				Expect(utils.Env.K8s.Create(ctx, work)).To(Succeed())
				utils.WaitWorkload(ctx, work.Name, "", 0, 2)
				started := time.Now().Add(-time.Second)
				By("starting the controller and checking the balanced scale-up plan")
				utils.StartController(ctx)
				counts := map[string]int{
					c.MainPool:     1,
					c.ZeroPool:     0,
					c.BalancePoolA: 1,
					c.BalancePoolB: 1,
				}
				var grown utils.Snapshot

				Eventually(
					ctx,
					func() error {
						var err error
						grown, err = utils.ReadActiveSnapshot(ctx)
						if err != nil {
							return err
						}

						for _, name := range []string{c.BalancePoolA, c.BalancePoolB} {
							if grown.Pools[name].Capacity > 1 || len(grown.Pools[name].Instances) > 1 {
								StopTrying("one pool received both new nodes").Now()
							}
						}

						return grown.StablePools(c, counts)
					},
					utils.SettleTimeout,
					utils.PollInterval,
				).Should(Succeed())
				pods := utils.WaitWorkload(ctx, work.Name, "", 2, 0)
				utils.AssertDistinctNodes(pods, 2)

				Eventually(
					ctx,
					func() error {
						logs, err := utils.Env.ReadControllerLogsSince(ctx, started)
						if err != nil {
							return err
						}

						return checkBalancedPlan(logs, c.BalancePoolA, c.BalancePoolB)
					},
					time.Minute,
					utils.PollInterval,
				).Should(Succeed())
				By("removing demand and waiting for both balance workers to be deleted")
				Expect(utils.Env.K8s.Delete(ctx, work)).To(Succeed())
				for _, pool := range []string{c.BalancePoolA, c.BalancePoolB} {
					utils.WaitDeleted(ctx, grown, pool, 1, 1, 0)
				}
			},
			NodeTimeout(40*time.Minute),
		)
	})
}
