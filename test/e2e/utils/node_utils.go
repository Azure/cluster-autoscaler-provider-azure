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

package utils

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	nodeutil "k8s.io/component-helpers/node/util"
	resourcehelper "k8s.io/component-helpers/resource"
	"sigs.k8s.io/yaml"
)

// ErrBounds marks a pool that has more VMs or requested capacity than its
// max tag allows.
var ErrBounds = errors.New("pool maximum exceeded")

const (
	// SuspendedCondition marks a retained parked Node.
	SuspendedCondition = corev1.NodeConditionType("Suspended")
	// SuspendedTaint prevents scheduling while a worker is parked or resuming.
	SuspendedTaint = "status-taint.cluster-autoscaler.kubernetes.io/azure-suspended"
	// ResumeAnnotation stores an in-flight resume marker.
	ResumeAnnotation = "cluster-autoscaler.kubernetes.io/azure-resume"
)

// NodeID returns the normalized Azure resource ID of node's VM.
func NodeID(node corev1.Node) string {
	return normalizeID(node.Spec.ProviderID)
}

// IsSuspended reports whether node has a true Suspended condition.
func IsSuspended(node corev1.Node) bool {
	_, condition := nodeutil.GetNodeCondition(&node.Status, SuspendedCondition)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

// HasSuspendedTaint reports whether node has the suspended NoSchedule taint.
func HasSuspendedTaint(node corev1.Node) bool {
	return slices.ContainsFunc(node.Spec.Taints, func(taint corev1.Taint) bool {
		return taint.Key == SuspendedTaint && taint.Effect == corev1.TaintEffectNoSchedule
	})
}

// Instance holds one VMSS VM's resource ID, VM ID, provisioning state and NICs.
type Instance struct {
	ID                string
	VMID              string
	PowerState        string
	ProvisioningState string
	NICs              []string
}

// PoolState holds one VMSS's capacity, bounds, template taint, state and VMs.
type PoolState struct {
	Capacity          int
	Max               int
	Min               int
	ProvisioningState string
	TemplateTaint     string
	Instances         map[string]Instance
}

// Snapshot is one reading of the pools and Nodes. It holds no credentials,
// bootstrap settings, Pod specs or logs.
type Snapshot struct {
	Pools map[string]PoolState
	Nodes []corev1.Node
}

// Ready reports whether node has a true Ready condition.
func Ready(node corev1.Node) bool {
	_, condition := nodeutil.GetNodeCondition(&node.Status, corev1.NodeReady)

	return condition != nil && condition.Status == corev1.ConditionTrue
}

// PoolNodes returns the Nodes whose provider ID is a VM of the VMSS with
// poolID.
func PoolNodes(nodes []corev1.Node, poolID string) []corev1.Node {
	var result []corev1.Node
	prefix := normalizeID(poolID) + "/virtualmachines/"

	for _, node := range nodes {
		if strings.HasPrefix(normalizeID(node.Spec.ProviderID), prefix) {
			result = append(result, node)
		}
	}

	return result
}

// WorkerNodes returns the Nodes of the test pools in Config.Pools.
func WorkerNodes(nodes []corev1.Node, c Config) []corev1.Node {
	var workers []corev1.Node

	for _, pool := range c.Pools() {
		workers = append(workers, PoolNodes(nodes, c.PoolID(pool))...)
	}

	return workers
}

// checkMax returns an ErrBounds error when a pool's capacity or VM count is
// above its max tag.
func (s Snapshot) checkMax() error {
	for name, pool := range s.Pools {
		if pool.Capacity > pool.Max || len(pool.Instances) > pool.Max {
			return fmt.Errorf(
				"%w: pool %s desired=%d actual=%d max=%d",
				ErrBounds,
				name,
				pool.Capacity,
				len(pool.Instances),
				pool.Max,
			)
		}
	}

	return nil
}

// Stable requires the test pools to have the given counts for their
// capacity, VMs and Ready, schedulable Nodes with the pool label, one Node
// for each VM, and a Succeeded VMSS.
func (s Snapshot) Stable(c Config, main, zero int) error {
	counts := map[string]int{c.MainPool: main, c.ZeroPool: zero}
	if c.BalancePoolA != "" {
		counts[c.BalancePoolA], counts[c.BalancePoolB] = 0, 0
	}

	return s.StablePools(c, counts)
}

// RunningStable excludes retained, settled parked Nodes from the running counts.
func (s Snapshot) RunningStable(c Config, main, zero int) error {
	// Copy the inventory before subtracting parked capacity.
	active := Snapshot{Pools: map[string]PoolState{}}
	for name, pool := range s.Pools {
		if pool.Capacity != len(pool.Instances) {
			return fmt.Errorf("pool %s has unsettled physical capacity", name)
		}
		pool.Instances = maps.Clone(pool.Instances)
		active.Pools[name] = pool
	}
	// Retain only running Nodes and reject unsettled parked VMs.
	for _, node := range s.Nodes {
		if !IsSuspended(node) {
			active.Nodes = append(active.Nodes, node)
			continue
		}

		id := normalizeID(node.Spec.ProviderID)
		found := false
		for name, pool := range active.Pools {
			vm, ok := pool.Instances[id]
			if !ok {
				continue
			}

			if vm.PowerState != "deallocated" || vm.ProvisioningState != "Succeeded" ||
				node.Annotations[ResumeAnnotation] != "" {
				return fmt.Errorf("suspended Node %s is not settled: power=%s provisioning=%s resume=%t",
					node.Name, vm.PowerState, vm.ProvisioningState, node.Annotations[ResumeAnnotation] != "")
			}

			if !HasSuspendedTaint(node) {
				return fmt.Errorf("parked Node %s has no suspended NoSchedule taint", node.Name)
			}
			delete(pool.Instances, id)
			pool.Capacity--
			active.Pools[name] = pool
			found = true
		}

		if !found {
			return fmt.Errorf("suspended Node %s has no retained VM", node.Name)
		}
	}

	return active.Stable(c, main, zero)
}

// StablePools checks capacity, VM and Ready Node identity for each pool.
func (s Snapshot) StablePools(c Config, counts map[string]int) error {
	for name := range counts {
		pool, ok := s.Pools[name]
		if !ok {
			return fmt.Errorf("pool %s is missing from the observation", name)
		}

		if pool.ProvisioningState != "Succeeded" {
			return fmt.Errorf("pool %s is %s, want Succeeded", name, pool.ProvisioningState)
		}
	}

	return s.readyPools(c, counts)
}

// Ready checks capacity and Ready Node identity without requiring an idle VMSS.
func (s Snapshot) Ready(c Config, main, zero int) error {
	counts := map[string]int{c.MainPool: main, c.ZeroPool: zero}
	if c.BalancePoolA != "" {
		counts[c.BalancePoolA] = 0
		counts[c.BalancePoolB] = 0
	}

	return s.readyPools(c, counts)
}

func (s Snapshot) readyPools(c Config, counts map[string]int) error {
	labels := map[string]string{
		c.MainPool:     MainLabel,
		c.ZeroPool:     ZeroLabel,
		c.BalancePoolA: BalanceLabel,
		c.BalancePoolB: BalanceLabel,
	}

	for name, count := range counts {
		pool, ok := s.Pools[name]
		if !ok {
			return fmt.Errorf("pool %s is missing from the observation", name)
		}

		if pool.Capacity != count || len(pool.Instances) != count {
			return fmt.Errorf("pool %s: desired=%d actual=%d, want %d", name, pool.Capacity, len(pool.Instances), count)
		}

		for _, instance := range pool.Instances {
			if instance.ProvisioningState == "Deleting" {
				return fmt.Errorf("pool %s: instance %s is deleting", name, instance.ID)
			}
		}

		nodes := PoolNodes(s.Nodes, c.PoolID(name))
		if len(nodes) != count {
			return fmt.Errorf("pool %s: %d registered Nodes, want %d", name, len(nodes), count)
		}

		seen := map[string]bool{}

		for _, node := range nodes {
			id := normalizeID(node.Spec.ProviderID)
			if _, ok := pool.Instances[id]; !ok || seen[id] || !Ready(node) ||
				node.Spec.Unschedulable || node.DeletionTimestamp != nil || node.Labels[PoolLabel] != labels[name] {
				return fmt.Errorf("pool %s: Node %s is not a unique Ready schedulable instance with the pool label", name, node.Name)
			}

			seen[id] = true
		}
	}

	return nil
}

// ValidateDemand requires node to have room for one Pod that requests
// demandMilliCPU, but not for two.
func ValidateDemand(node corev1.Node, pods []corev1.Pod, demandMilliCPU int64) error {
	occupied, err := NodeRequests(node, pods)
	if err != nil {
		return err
	}

	available := node.Status.Allocatable.Cpu().MilliValue() - occupied.Cpu().MilliValue()
	if available < demandMilliCPU || available >= 2*demandMilliCPU {
		return fmt.Errorf(
			"node %s has %dm free CPU; require demand <= free < 2*demand (%dm)",
			node.Name,
			available,
			demandMilliCPU,
		)
	}

	return nil
}

// NodeRequests sums the requests of the active Pods on node, including init
// containers, sidecars and overhead.
func NodeRequests(node corev1.Node, pods []corev1.Pod) (corev1.ResourceList, error) {
	occupied := corev1.ResourceList{}

	for _, pod := range pods {
		if pod.Spec.NodeName != node.Name || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		// This fixture does not use Pod-level request accounting.
		if pod.Spec.Resources != nil {
			return nil, fmt.Errorf(
				"node %s has Pod %s/%s with unsupported Pod-level requests",
				node.Name,
				pod.Namespace,
				pod.Name,
			)
		}

		requests := resourcehelper.PodRequests(&pod, resourcehelper.PodResourcesOptions{})

		for name, request := range requests {
			sum := occupied[name]
			sum.Add(request)
			occupied[name] = sum
		}
	}

	return occupied, nil
}

// CheckStatus requires the autoscaler status to be less than two minutes old,
// Running, written after the controller Pod was created and listing the expected groups.
func CheckStatus(status string, expected []string, now, podCreated time.Time) error {
	var parsed struct {
		Time             string `json:"time"`
		AutoscalerStatus string `json:"autoscalerStatus"`
		NodeGroups       []struct {
			Name string `json:"name"`
		} `json:"nodeGroups"`
	}

	if err := yaml.Unmarshal([]byte(status), &parsed); err != nil {
		return fmt.Errorf("decode autoscaler status: %w", err)
	}

	at, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", parsed.Time)
	if err != nil {
		return fmt.Errorf("autoscaler status time %q is missing or invalid: %w", parsed.Time, err)
	}

	if now.Sub(at) > 2*time.Minute || at.After(now.Add(10*time.Second)) {
		return fmt.Errorf("autoscaler status time %s is stale at %s", at.Format(time.RFC3339), now.Format(time.RFC3339))
	}

	if at.Before(podCreated) {
		return fmt.Errorf("autoscaler status time %s predates controller Pod creation %s", at, podCreated)
	}

	if parsed.AutoscalerStatus != "Running" || len(parsed.NodeGroups) != len(expected) {
		return fmt.Errorf(
			"autoscaler must be Running with exactly %d groups, got %s with %d groups",
			len(expected),
			parsed.AutoscalerStatus,
			len(parsed.NodeGroups),
		)
	}

	names := map[string]bool{}

	for _, group := range parsed.NodeGroups {
		names[group.Name] = true
	}

	for _, name := range expected {
		if !names[name] {
			return fmt.Errorf("autoscaler did not report expected group %s", name)
		}
	}

	return nil
}
