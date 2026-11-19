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
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

// RegisterMinimum adds the pool minimum enforcement phase.
func RegisterMinimum() {
	Describe("minimum phase", func() {
		BeforeAll(func(ctx SpecContext) {
			Expect(utils.Env.PreparePhase(ctx, utils.TestSuiteLabelMinimum)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(utils.Env.RestorePhase(ctx, utils.TestSuiteLabelMinimum)).To(Succeed())
			}, NodeTimeout(30*time.Minute))
		}, NodeTimeout(75*time.Minute))
		BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
		It(
			"grows a pool from one worker to its configured minimum of two",
			Label(utils.TestSuiteLabelMinimum, utils.TestSuiteLabelSlow),
			func(ctx SpecContext) {
				By("checking the raised minimum without pending demand")
				before := utils.WaitStable(ctx, 1, 0)
				Expect(before.Pools[utils.Env.Config.MainPool].Min).To(Equal(2))
				Expect(utils.ControllerReplicas(ctx)).To(Equal(int32(0)))
				Expect(utils.NoPendingDemand(ctx)).To(Succeed())
				started := time.Now().Add(-time.Second)
				By("starting the controller and waiting for minimum-size growth")
				utils.StartController(ctx)
				grown := utils.WaitStable(ctx, 2, 0)

				for id, instance := range before.Pools[utils.Env.Config.MainPool].Instances {
					Expect(grown.Pools[utils.Env.Config.MainPool].Instances[id].VMID).To(Equal(instance.VMID))
				}

				Eventually(
					ctx,
					func() error {
						logs, err := utils.Env.ReadControllerLogsSince(ctx, started)
						if err != nil {
							return err
						}

						for _, line := range strings.Split(logs, "\n") {
							if strings.Contains(line, "ScaleUpToNodeGroupMinSize: final scale-up plan") &&
								strings.Contains(line, utils.Env.Config.MainPool+" 1->2") {
								return nil
							}
						}

						return fmt.Errorf("no minimum-size scale-up plan for main")
					},
					time.Minute,
					utils.PollInterval,
				).Should(Succeed())

				By("checking that the new minimum is retained without demand")
				Consistently(
					ctx,
					func() error {
						current, err := utils.ReadActiveSnapshot(ctx)
						if err != nil {
							return err
						}

						if err := utils.NoPendingDemand(ctx); err != nil {
							return err
						}

						return current.Ready(utils.Env.Config, 2, 0)
					},
					time.Minute,
					utils.PollInterval,
				).Should(Succeed())
			},
			NodeTimeout(25*time.Minute),
		)
	})
}
