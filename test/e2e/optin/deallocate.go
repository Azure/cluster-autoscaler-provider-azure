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

package optin

import (
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

// RegisterDeallocate adds the parked worker resume phase.
func RegisterDeallocate() {
	Describe("deallocate phase", func() {
		BeforeAll(func(ctx SpecContext) {
			if utils.Env.Config.DeallocateImage == "" {
				Skip("requires deallocateImage")
			}

			Expect(utils.Env.PreparePhase(ctx, utils.TestSuiteLabelDeallocate)).To(Succeed())
		}, NodeTimeout(75*time.Minute))
		BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
		It(
			"keeps parked Nodes marked Suspended and starts parked VMs before creating new ones",
			Label(utils.TestSuiteLabelDeallocate, utils.TestSuiteLabelSlow),
			func(ctx SpecContext) {
				c := utils.Env.Config
				container := utils.ControllerContainer(ctx)
				Expect(container.Env).To(ContainElement(corev1.EnvVar{Name: "AZURE_DEALLOCATE", Value: "true"}))
				flags := slices.Concat(container.Command, container.Args)
				Expect(flags).To(ContainElement("--startup-taint=node.cloudprovider.kubernetes.io/shutdown"))
				Expect(flags).To(ContainElement("--startup-taint=node.kubernetes.io/out-of-service"))
				before := utils.WaitStable(ctx, 1, 0)
				Expect(before.Pools[c.MainPool].Max).To(Equal(3))
				Expect(utils.Env.DemandFits(ctx, before, c.MainPool)).To(Succeed())
				work := utils.Env.Deployment(utils.Namespace.Name, "park-demand", utils.MainLabel, utils.DemandCPU, 2)
				Expect(utils.Env.K8s.Create(ctx, work)).To(Succeed())
				grown := utils.WaitStable(ctx, 2, 0)
				utils.WaitWorkload(ctx, work.Name, c.MainPool, 2, 0)
				utils.ScaleDeployment(ctx, work, 0)

				Eventually(
					ctx,
					func() ([]corev1.Pod, error) { return utils.ListWorkload(ctx, work.Name) },
					3*time.Minute,
					utils.PollInterval,
				).Should(BeEmpty())

				var parked utils.Snapshot

				Eventually(
					ctx,
					func() error {
						var err error
						parked, err = utils.ReadActiveSnapshot(ctx)
						if err != nil {
							return err
						}

						return checkParkedPool(grown, parked, c, 1)
					},
					utils.SettleTimeout,
					utils.PollInterval,
				).Should(Succeed())

				Consistently(
					ctx,
					func() error {
						current, err := utils.ReadActiveSnapshot(ctx)
						if err != nil {
							return err
						}

						return checkParkedPool(grown, current, c, 1)
					},
					time.Minute,
					utils.PollInterval,
				).Should(Succeed())

				AddReportEntry("parked-node-retention", parked)

				By("resuming the parked VM and creating only the remaining required worker")
				started := time.Now().Add(-time.Second)
				utils.ScaleDeployment(ctx, work, 3)
				resumed := utils.WaitStable(ctx, 3, 0)
				utils.WaitWorkload(ctx, work.Name, c.MainPool, 3, 0)

				for id, original := range grown.Pools[c.MainPool].Instances {
					Expect(resumed.Pools[c.MainPool].Instances[id].VMID).To(Equal(original.VMID))
				}

				for _, node := range utils.PoolNodes(grown.Nodes, c.PoolID(c.MainPool)) {
					var current corev1.Node
					Expect(utils.Env.K8s.Get(ctx, client.ObjectKey{Name: node.Name}, &current)).To(Succeed())
					Expect(current.UID).To(Equal(node.UID))
					Expect(current.Spec.PodCIDR).To(Equal(node.Spec.PodCIDR))
					Expect(isSuspended(current)).To(BeFalse())
					Expect(utils.Ready(current)).To(BeTrue())
				}

				var parkedID string

				for id, instance := range parked.Pools[c.MainPool].Instances {
					if instance.PowerState == "deallocated" {
						parkedID = id
					}
				}

				Expect(parkedID).NotTo(BeEmpty())
				var operations []azureOperation

				Eventually(
					ctx,
					func() error {
						events, err := utils.Env.ActivityLogs(ctx, started)
						if err != nil {
							return err
						}

						operations = nil

						for _, event := range events {
							if event == nil {
								continue
							}

							operation := azureOperation{
								At:       ptr.Deref(event.EventTimestamp, time.Time{}),
								Resource: ptr.Deref(event.ResourceID, ""),
								ID:       ptr.Deref(event.OperationID, ""),
							}
							if event.OperationName != nil {
								operation.Operation = ptr.Deref(event.OperationName.Value, "")
							}

							if event.Status != nil {
								operation.Status = ptr.Deref(event.Status.Value, "")
							}

							operations = append(operations, operation)
						}

						return checkParkedFirst(operations, c.PoolID(c.MainPool), parkedID)
					},
					8*time.Minute,
					30*time.Second,
				).Should(Succeed())

				AddReportEntry("azure-start-and-growth", operations)

				AddReportEntry("parked-first", "one Start requested before one VMSS growth; original Node and VM identities retained")
			},
			NodeTimeout(50*time.Minute),
		)
	})
}

const suspendedCondition = corev1.NodeConditionType("Suspended")

type azureOperation struct {
	At        time.Time `json:"at"`
	Resource  string    `json:"resource"`
	Operation string    `json:"operation"`
	Status    string    `json:"status"`
	ID        string    `json:"id"`
}

func checkParkedFirst(operations []azureOperation, poolID, parkedID string) error {
	starts := map[string]time.Time{}
	var growth time.Time

	for _, operation := range operations {
		if operation.Status != "Started" {
			continue
		}

		if strings.EqualFold(operation.Operation, "Microsoft.Compute/virtualMachineScaleSets/virtualMachines/start/action") &&
			strings.EqualFold(operation.Resource, parkedID) {
			if operation.ID == "" {
				return fmt.Errorf("Start evidence has no operation ID")
			}

			starts[operation.ID] = operation.At
		}

		if strings.EqualFold(operation.Operation, "Microsoft.Compute/virtualMachineScaleSets/write") &&
			strings.EqualFold(operation.Resource, poolID) && (growth.IsZero() || operation.At.Before(growth)) {
			growth = operation.At
		}
	}

	if len(starts) != 1 || growth.IsZero() {
		return fmt.Errorf("waiting for exactly one parked VM Start and a VMSS growth request, got %d Starts", len(starts))
	}

	for _, start := range starts {
		if start.IsZero() || !start.Before(growth) {
			return fmt.Errorf("VMSS growth was not requested after the parked VM Start")
		}
	}

	return nil
}

// checkParkedPool requires retained VM/Node identities and exactly parked
// deallocated VMs, with the running minimum still Ready.
func checkParkedPool(before, after utils.Snapshot, c utils.Config, parked int) error {
	old, pool := before.Pools[c.MainPool], after.Pools[c.MainPool]
	if pool.Capacity != old.Capacity || len(pool.Instances) != len(old.Instances) {
		return fmt.Errorf("parking changed physical pool capacity")
	}

	nodes := utils.PoolNodes(after.Nodes, c.PoolID(c.MainPool))
	if len(nodes) != len(before.Pools[c.MainPool].Instances) {
		return fmt.Errorf("parking deleted a Node")
	}

	suspended, running := 0, 0

	for _, original := range utils.PoolNodes(before.Nodes, c.PoolID(c.MainPool)) {
		found := false

		for _, node := range nodes {
			if node.Name != original.Name {
				continue
			}

			found = true
			id := strings.ToLower(strings.TrimPrefix(node.Spec.ProviderID, "azure://"))
			vm, ok := pool.Instances[id]
			if !ok || vm.VMID != old.Instances[id].VMID || node.UID != original.UID ||
				node.Spec.ProviderID != original.Spec.ProviderID || node.Spec.PodCIDR != original.Spec.PodCIDR {
				return fmt.Errorf("parked Node or VM identity changed")
			}

			if vm.PowerState == "deallocated" && isSuspended(node) {
				suspended++
			} else if vm.PowerState == "running" && utils.Ready(node) && !isSuspended(node) {
				running++
			} else {
				return fmt.Errorf(
					"Node %s has power=%s, Suspended=%t, Ready=%t",
					node.Name,
					vm.PowerState,
					isSuspended(node),
					utils.Ready(node),
				)
			}
		}

		if !found {
			return fmt.Errorf("original Node %s is missing", original.Name)
		}
	}

	if suspended != parked || running != pool.Min {
		return fmt.Errorf("pool has %d suspended and %d running, want %d and min %d", suspended, running, parked, pool.Min)
	}

	return nil
}

func isSuspended(node corev1.Node) bool {
	_, condition := nodeutil.GetNodeCondition(&node.Status, suspendedCondition)

	return condition != nil && condition.Status == corev1.ConditionTrue
}
