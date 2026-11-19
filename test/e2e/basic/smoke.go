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

package basic

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

var _ = Describe("Azure Provider", func() {
	BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
	It(
		"discovers only the tagged node groups and stays idle for five minutes",
		Label(utils.TestSuiteLabelSmoke, "idle"),
		func(ctx SpecContext) {
			Consistently(
				ctx,
				func() error {
					snapshot, err := utils.ReadActiveSnapshot(ctx)
					if err != nil {
						return err
					}

					return snapshot.Ready(utils.Env.Config, 1, 0)
				},
				5*time.Minute,
				utils.PollInterval,
			).Should(Succeed())
		},
		NodeTimeout(7*time.Minute),
	)

	It(
		"scales up for pending Pods, stops at the node group maximum and deletes the extra node",
		Label(utils.TestSuiteLabelSmoke, "max-size", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			Expect(utils.Env.DemandFits(ctx, utils.WaitStable(ctx, 1, 0), utils.Env.Config.MainPool)).To(Succeed())

			By("creating demand for two main workers")
			deployment := utils.Env.Deployment(utils.Namespace.Name, "demand", utils.MainLabel, utils.DemandCPU, 2)
			Expect(utils.Env.K8s.Create(ctx, deployment)).To(Succeed())
			grown := utils.WaitStable(ctx, 2, 0)
			pods := utils.WaitWorkload(ctx, deployment.Name, utils.Env.Config.MainPool, 2, 0)
			Expect(pods[0].Spec.NodeName).NotTo(Equal(pods[1].Spec.NodeName))

			By("adding a third Pod and checking that main stays at its maximum")
			utils.ScaleDeployment(ctx, deployment, 3)
			utils.WaitWorkload(ctx, deployment.Name, utils.Env.Config.MainPool, 2, 1)

			Consistently(ctx, func() error {
				snapshot, err := utils.ReadActiveSnapshot(ctx)
				if err != nil {
					return err
				}

				if err := snapshot.Ready(utils.Env.Config, 2, 0); err != nil {
					return err
				}

				_, err = utils.Env.WorkloadState(ctx, utils.Namespace.Name, deployment.Name, utils.Env.Config.MainPool, 2, 1)

				return err
			}, 2*time.Minute, utils.PollInterval).Should(Succeed())

			By("removing the demand and waiting for the added worker to be deleted")
			Expect(utils.Env.K8s.Delete(ctx, deployment)).To(Succeed())
			utils.WaitDeleted(ctx, grown, utils.Env.Config.MainPool, 1, 1, 0)
		},
		NodeTimeout(50*time.Minute),
	)

	It(
		"scales a node group up from zero and deletes the VM, Node and NIC when it returns to zero",
		Label(utils.TestSuiteLabelSmoke, "scale-from-zero", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			By("creating demand for the zero pool")
			deployment := utils.Env.Deployment(utils.Namespace.Name, "zero", utils.ZeroLabel, utils.DemandCPU, 1)
			Expect(utils.Env.K8s.Create(ctx, deployment)).To(Succeed())

			By("waiting for the zero pool to grow to one worker")
			grown := utils.WaitStable(ctx, 1, 1)
			utils.WaitWorkload(ctx, deployment.Name, utils.Env.Config.ZeroPool, 1, 0)

			By("removing the demand and waiting for the zero-pool worker to be deleted")
			Expect(utils.Env.K8s.Delete(ctx, deployment)).To(Succeed())
			utils.WaitDeleted(ctx, grown, utils.Env.Config.ZeroPool, 1, 1, 0)
		},
		NodeTimeout(40*time.Minute),
	)

})
