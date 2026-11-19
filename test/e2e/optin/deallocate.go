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
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

const (
	suspendedCondition = utils.SuspendedCondition
	suspendedTaint     = utils.SuspendedTaint
	resumeAnnotation   = utils.ResumeAnnotation
	disabledAnnotation = "cluster-autoscaler.kubernetes.io/scale-down-disabled"
	// resumeMarkerGrace keeps slow Azure reads out of the first marker observations.
	resumeMarkerGrace = time.Minute
)

type resumeMarker struct {
	VMID    string    `json:"vmID"`
	NodeUID types.UID `json:"nodeUID"`
	Started time.Time `json:"started"`
	Repark  bool      `json:"repark,omitempty"`
}

// RegisterDeallocate adds the parked worker resume phases.
func RegisterDeallocate() {
	Describe("deallocate phase", func() {
		BeforeAll(func(ctx SpecContext) {
			if utils.Env.Config.DeallocateImage == "" {
				Skip("requires deallocateImage")
			}
			Expect(utils.Env.PreparePhase(ctx, utils.TestSuiteLabelDeallocate)).To(Succeed())
		}, NodeTimeout(75*time.Minute))
		BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))

		It("resumes a parked VM in place without adding capacity",
			Label(utils.TestSuiteLabelDeallocate, utils.TestSuiteLabelSlow),
			func(ctx SpecContext) {
				work, parked, original := parkWorker(ctx)
				marker := startResume(ctx, work, parked, original)
				waitResumed(ctx, work, parked, original, marker)
			}, NodeTimeout(40*time.Minute))

		It("backs off a failed resume and re-parks the suspended Node",
			Label(utils.TestSuiteLabelDeallocate, utils.TestSuiteLabelSlow),
			func(ctx SpecContext) {
				work, parked, original := parkWorker(ctx)
				Expect(utils.Env.SetResumeDeadline(ctx, "10s")).To(Succeed())
				DeferCleanup(func(ctx SpecContext) {
					Expect(utils.Env.SetResumeDeadline(ctx, "")).To(Succeed())
					restartController(ctx)
					Eventually(ctx, utils.Env.Controller, utils.ControllerTimeout, time.Second).Should(Succeed())
				}, NodeTimeout(4*time.Minute))
				// Restart so the next resume reads the new per-pool deadline.
				restartController(ctx)
				Eventually(ctx, utils.Env.Controller, utils.ControllerTimeout, time.Second).Should(Succeed())
				started := time.Now()
				marker := startResume(ctx, work, parked, original)
				repark, backedOff := false, false

				Eventually(ctx, func() error {
					current := readRetainedNode(ctx, original)
					Expect(suspensionError(current)).To(Succeed(), "failed resume must never make the Node schedulable")
					pods, err := utils.ListWorkload(ctx, work.Name)
					if err != nil {
						return err
					}

					for _, pod := range pods {
						Expect(pod.Spec.NodeName).NotTo(Equal(original.Name), "a demand Pod bound to the failed resume")
					}

					if value := current.Annotations[resumeAnnotation]; value != "" {
						observed, err := checkResumeMarker(current, parked.Pools[utils.Env.Config.MainPool].Instances[nodeID(original)])
						Expect(err).NotTo(HaveOccurred())
						Expect(observed.Started).To(Equal(marker.Started), "resume deadline changed")
						if observed.Repark && !repark {
							repark = true
							AddReportEntry("failed-resume-repark-marker", observed)
							utils.ScaleDeployment(ctx, work, 0)
						}
					} else if !repark {
						StopTrying("resume marker cleared before re-park was observed").Now()
					}

					var status corev1.ConfigMap
					if err := utils.Env.K8s.Get(ctx, client.ObjectKey{Namespace: utils.AutoscalerNamespace, Name: "cluster-autoscaler-status"}, &status); err != nil {
						return err
					}

					if !backedOff {
						backedOff, err = groupBackedOff(status.Data["status"], utils.Env.Config.MainPool, started)
						if err != nil {
							return err
						}

						if backedOff {
							AddReportEntry("failed-resume-group-backoff", status.Data["status"])
						}
					}

					if !repark || !backedOff || current.Annotations[resumeAnnotation] != "" {
						return fmt.Errorf("waiting for re-park, group backoff and marker removal (repark=%t backoff=%t)", repark, backedOff)
					}

					snapshot, err := utils.ReadSnapshot(ctx)
					if err != nil {
						return err
					}

					vm := snapshot.Pools[utils.Env.Config.MainPool].Instances[nodeID(original)]
					Expect(vm.VMID).To(Equal(parked.Pools[utils.Env.Config.MainPool].Instances[nodeID(original)].VMID))
					if vm.PowerState != "deallocated" || vm.ProvisioningState != "Succeeded" {
						return fmt.Errorf("re-park has not settled: power=%s provisioning=%s", vm.PowerState, vm.ProvisioningState)
					}
					AddReportEntry("failed-resume-deallocated", vm)
					return nil
				}, utils.SettleTimeout, time.Second).Should(Succeed())
			}, NodeTimeout(45*time.Minute))

		It("recovers the same in-flight resume after the autoscaler Pod restarts",
			Label(utils.TestSuiteLabelDeallocate, utils.TestSuiteLabelSlow),
			func(ctx SpecContext) {
				work, parked, original := parkWorker(ctx)
				marker := startResume(ctx, work, parked, original)
				restartController(ctx)
				waitResumed(ctx, work, parked, original, marker)
				Eventually(ctx, utils.Env.Controller, utils.ControllerTimeout, time.Second).Should(Succeed())
			}, NodeTimeout(40*time.Minute))
	})
}

// parkWorker grows main once, removes demand and observes one retained parked Node.
func parkWorker(ctx SpecContext) (*appsv1.Deployment, utils.Snapshot, corev1.Node) {
	By("growing main and removing demand to park one worker")
	c := utils.Env.Config
	container := utils.ControllerContainer(ctx)
	Expect(container.Env).To(ContainElement(corev1.EnvVar{Name: "AZURE_DEALLOCATE", Value: "true"}))
	flags := slices.Concat(container.Command, container.Args)
	Expect(flags).To(ContainElement("--startup-taint=node.cloudprovider.kubernetes.io/shutdown"))
	Expect(flags).To(ContainElement("--startup-taint=node.kubernetes.io/out-of-service"))
	before := utils.WaitStable(ctx, 1, 0)
	Expect(utils.Env.DemandFits(ctx, before, c.MainPool)).To(Succeed())
	work := utils.Env.Deployment(utils.Namespace.Name, "park-demand", utils.MainLabel, utils.DemandCPU, 2)
	Expect(utils.Env.K8s.Create(ctx, work)).To(Succeed())
	grown := utils.WaitStable(ctx, 2, 0)
	utils.WaitWorkload(ctx, work.Name, c.MainPool, 2, 0)
	utils.ScaleDeployment(ctx, work, 0)
	Eventually(ctx, func() ([]corev1.Pod, error) {
		return utils.ListWorkload(ctx, work.Name)
	}, 3*time.Minute, utils.PollInterval).Should(BeEmpty())

	By("checking retained identities and settled suspension")
	var parked utils.Snapshot
	Eventually(ctx, func() error {
		var err error
		parked, err = utils.ReadActiveSnapshot(ctx)
		if err != nil {
			return err
		}

		return checkParkedPool(grown, parked, c, len(grown.Pools[c.MainPool].Instances)-1)
	}, utils.SettleTimeout, utils.PollInterval).Should(Succeed())
	Consistently(ctx, func() error {
		current, err := utils.ReadActiveSnapshot(ctx)
		if err != nil {
			return err
		}

		for _, node := range utils.PoolNodes(parked.Nodes, c.PoolID(c.MainPool)) {
			if parked.Pools[c.MainPool].Instances[nodeID(node)].PowerState == "deallocated" {
				checkExternalStart(ctx, node, current)
			}
		}

		return checkParkedPool(grown, current, c, len(grown.Pools[c.MainPool].Instances)-1)
	}, time.Minute, utils.PollInterval).Should(Succeed())
	AddReportEntry("parked-node-retention", parked)
	for _, node := range utils.PoolNodes(parked.Nodes, c.PoolID(c.MainPool)) {
		if parked.Pools[c.MainPool].Instances[nodeID(node)].PowerState == "deallocated" {
			return work, parked, node
		}
	}
	Fail("no retained parked Node")
	return nil, utils.Snapshot{}, corev1.Node{}
}

// startResume adds demand and captures the retained Node's initial resume marker.
func startResume(ctx SpecContext, work *appsv1.Deployment, parked utils.Snapshot, original corev1.Node) resumeMarker {
	By("adding demand and observing the initial resume marker")
	utils.ScaleDeployment(ctx, work, 2)
	var marker resumeMarker
	lastCloudRead := time.Now().Add(resumeMarkerGrace - utils.PollInterval)
	Eventually(ctx, func() error {
		current := readRetainedNode(ctx, original)
		if current.Annotations[resumeAnnotation] != "" {
			var err error
			marker, err = checkResumeMarker(current, parked.Pools[utils.Env.Config.MainPool].Instances[nodeID(original)])
			Expect(err).NotTo(HaveOccurred())
			Expect(marker.Repark).To(BeFalse(), "initial resume was already re-parking")
			Expect(suspensionError(current)).To(Succeed())
			AddReportEntry("resume-marker", marker)
			return nil
		}
		Expect(suspensionError(current)).To(Succeed(), "parked Node lost suspension without an observed resume marker")
		if time.Since(lastCloudRead) >= utils.PollInterval {
			snapshot, err := utils.ReadSnapshot(ctx)
			if err != nil {
				return err
			}
			lastCloudRead = time.Now()
			checkExternalStart(ctx, original, snapshot)
			Expect(checkCapacity(parked, snapshot, utils.Env.Config.MainPool)).To(Succeed())
		}

		return fmt.Errorf("waiting for resume marker on %s", original.Name)
	}, utils.SettleTimeout, 250*time.Millisecond).Should(Succeed())
	return marker
}

// waitResumed requires the same Node and VM to become usable without new capacity.
func waitResumed(ctx SpecContext, work *appsv1.Deployment, parked utils.Snapshot, original corev1.Node, marker resumeMarker) {
	By("waiting for the retained Node to resume without physical growth")
	var lastCloudRead time.Time
	Eventually(ctx, func() error {
		current := readRetainedNode(ctx, original)
		value := current.Annotations[resumeAnnotation]
		if value != "" {
			observed, err := checkResumeMarker(current, parked.Pools[utils.Env.Config.MainPool].Instances[nodeID(original)])
			Expect(err).NotTo(HaveOccurred())
			Expect(observed.Started).To(Equal(marker.Started), "restart must preserve the original deadline")
			Expect(observed.Repark).To(BeFalse(), "happy-path resume failed")
			Expect(hasSuspendedTaint(current)).To(BeTrue(), "Node became schedulable during resume")
			Expect(current.Annotations[disabledAnnotation]).To(Equal("true"))
		}

		if value != "" && isSuspended(current) && time.Since(lastCloudRead) < utils.PollInterval {
			return fmt.Errorf("Node %s is still resuming", original.Name)
		}

		snapshot, err := utils.ReadSnapshot(ctx)
		if err != nil {
			return err
		}
		lastCloudRead = time.Now()
		Expect(checkCapacity(parked, snapshot, utils.Env.Config.MainPool)).To(Succeed())
		if value != "" {
			Expect(checkResumeProtection(current, snapshot.Pools[utils.Env.Config.MainPool].Instances[nodeID(original)])).To(Succeed())
			return fmt.Errorf("Node %s is still resuming", original.Name)
		}

		vm := snapshot.Pools[utils.Env.Config.MainPool].Instances[nodeID(original)]
		Expect(vm.PowerState).To(Equal("running"), "Node unsuspended before its VM was running")
		Expect(vm.ProvisioningState).To(Equal("Succeeded"))
		Expect(utils.Ready(current)).To(BeTrue(), "Node unsuspended before Ready")
		Expect(isSuspended(current)).To(BeFalse())
		Expect(hasSuspendedTaint(current)).To(BeFalse())
		Expect(current.Annotations[disabledAnnotation]).NotTo(Equal("true"))
		AddReportEntry("resumed-node-and-vm", struct {
			NodeUID types.UID
			VM      utils.Instance
		}{current.UID, vm})
		return nil
	}, utils.SettleTimeout, time.Second).Should(Succeed())
	pods := utils.WaitWorkload(ctx, work.Name, utils.Env.Config.MainPool, 2, 0)
	Expect(slices.ContainsFunc(pods, func(pod corev1.Pod) bool {
		return pod.Spec.NodeName == original.Name
	})).To(BeTrue(), "no demand Pod runs on the resumed Node")
	snapshot, err := utils.ReadSnapshot(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(checkCapacity(parked, snapshot, utils.Env.Config.MainPool)).To(Succeed())
}

func readRetainedNode(ctx SpecContext, original corev1.Node) corev1.Node {
	var current corev1.Node
	Expect(utils.Env.K8s.Get(ctx, client.ObjectKey{Name: original.Name}, &current)).To(Succeed())
	Expect(current.UID).To(Equal(original.UID), "resume replaced the Node")
	Expect(current.Spec.ProviderID).To(Equal(original.Spec.ProviderID))
	Expect(current.Spec.PodCIDR).To(Equal(original.Spec.PodCIDR))
	return current
}

func restartController(ctx SpecContext) {
	utils.Env.SaveControllerLogs(ctx)
	pods, err := utils.Env.ControllerPods(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(pods).To(HaveLen(1))
	Expect(utils.Env.K8s.Delete(ctx, &pods[0], client.Preconditions{UID: &pods[0].UID})).To(Succeed())
	AddReportEntry("deleted-autoscaler-pod", pods[0].Name)
}

func checkExternalStart(ctx SpecContext, original corev1.Node, snapshot utils.Snapshot) {
	vm := snapshot.Pools[utils.Env.Config.MainPool].Instances[nodeID(original)]
	if vm.PowerState != "deallocated" && vm.PowerState != "deallocating" {
		current := readRetainedNode(ctx, original)
		if current.Annotations[resumeAnnotation] == "" {
			StopTrying(fmt.Sprintf("parked VM %s started without an azure-resume marker (power=%s): external Start, such as AKS/ASO reconciliation; collect the Azure Activity Log", vm.ID, vm.PowerState)).Now()
		}
	}
}

func checkResumeMarker(node corev1.Node, vm utils.Instance) (resumeMarker, error) {
	var marker resumeMarker
	if err := json.Unmarshal([]byte(node.Annotations[resumeAnnotation]), &marker); err != nil {
		return marker, fmt.Errorf("decode resume marker: %w", err)
	}

	if marker.VMID == "" || marker.VMID != vm.VMID || marker.NodeUID != node.UID || marker.Started.IsZero() {
		return marker, fmt.Errorf("resume marker does not identify the retained VM/Node and start time")
	}

	return marker, nil
}

func checkCapacity(before, after utils.Snapshot, pool string) error {
	old, current := before.Pools[pool], after.Pools[pool]
	if old.Capacity != current.Capacity || len(old.Instances) != len(current.Instances) {
		return fmt.Errorf("resume added or removed physical VMSS capacity")
	}

	for id, vm := range old.Instances {
		if current.Instances[id].VMID != vm.VMID {
			return fmt.Errorf("resume replaced VM %s", id)
		}
	}

	return nil
}

// checkParkedPool requires retained identities, settled parked VMs and the running minimum.
func checkParkedPool(before, after utils.Snapshot, c utils.Config, parked int) error {
	if err := checkCapacity(before, after, c.MainPool); err != nil {
		return err
	}

	nodes := utils.PoolNodes(after.Nodes, c.PoolID(c.MainPool))
	originals := utils.PoolNodes(before.Nodes, c.PoolID(c.MainPool))
	if len(nodes) != len(originals) {
		return fmt.Errorf("parking deleted a Node")
	}

	suspended, running := 0, 0
	for _, original := range originals {
		index := slices.IndexFunc(nodes, func(node corev1.Node) bool { return node.Name == original.Name })
		if index < 0 {
			return fmt.Errorf("original Node %s is missing", original.Name)
		}

		node := nodes[index]
		if node.UID != original.UID || node.Spec.ProviderID != original.Spec.ProviderID || node.Spec.PodCIDR != original.Spec.PodCIDR {
			return fmt.Errorf("parked Node identity changed")
		}

		vm := after.Pools[c.MainPool].Instances[nodeID(node)]
		if node.Annotations[resumeAnnotation] != "" {
			return fmt.Errorf("Node %s still has a resume marker", node.Name)
		}

		if vm.PowerState == "deallocated" && vm.ProvisioningState == "Succeeded" {
			if err := suspensionError(node); err != nil {
				return err
			}
			suspended++
		} else if vm.PowerState == "running" && utils.Ready(node) && !isSuspended(node) && !hasSuspendedTaint(node) {
			running++
		} else {
			return fmt.Errorf("Node %s has power=%s Suspended=%t Ready=%t", node.Name, vm.PowerState, isSuspended(node), utils.Ready(node))
		}
	}

	if suspended != parked || running != after.Pools[c.MainPool].Min {
		return fmt.Errorf("pool has %d suspended and %d running, want %d and min %d", suspended, running, parked, after.Pools[c.MainPool].Min)
	}

	return nil
}

// groupBackedOff reports a scale-up backoff observed after since.
func groupBackedOff(status, pool string, since time.Time) (bool, error) {
	var parsed struct {
		NodeGroups []struct {
			Name    string `json:"name"`
			ScaleUp struct {
				Status        string    `json:"status"`
				LastProbeTime time.Time `json:"lastProbeTime"`
			} `json:"scaleUp"`
		} `json:"nodeGroups"`
	}

	if err := yaml.Unmarshal([]byte(status), &parsed); err != nil {
		return false, fmt.Errorf("decode autoscaler backoff status: %w", err)
	}

	for _, group := range parsed.NodeGroups {
		if strings.EqualFold(group.Name, pool) {
			return group.ScaleUp.Status == "Backoff" && group.ScaleUp.LastProbeTime.After(since), nil
		}
	}

	return false, fmt.Errorf("autoscaler status has no group %s", pool)
}

func checkResumeProtection(node corev1.Node, vm utils.Instance) error {
	if !hasSuspendedTaint(node) || node.Annotations[disabledAnnotation] != "true" {
		return fmt.Errorf("resuming Node %s lost its scheduling or scale-down protection", node.Name)
	}

	if !isSuspended(node) && (!utils.Ready(node) || vm.PowerState != "running" || vm.ProvisioningState != "Succeeded") {
		return fmt.Errorf("resuming Node %s lost Suspended before Ready and Running/Succeeded", node.Name)
	}

	return nil
}

func nodeID(node corev1.Node) string {
	return utils.NodeID(node)
}

func suspensionError(node corev1.Node) error {
	if !isSuspended(node) || !hasSuspendedTaint(node) || node.Annotations[disabledAnnotation] != "true" {
		return fmt.Errorf("Node %s lost suspension condition, NoSchedule taint or scale-down protection", node.Name)
	}

	return nil
}

func hasSuspendedTaint(node corev1.Node) bool {
	return utils.HasSuspendedTaint(node)
}

func isSuspended(node corev1.Node) bool {
	return utils.IsSuspended(node)
}
