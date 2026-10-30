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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSnapshotStableCounts(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		main, zero, wantMain, wantZero int
		valid                          bool
	}{
		{
			main:     1,
			zero:     0,
			wantMain: 1,
			wantZero: 0,
			valid:    true,
		},
		{
			main:     2,
			zero:     1,
			wantMain: 2,
			wantZero: 1,
			valid:    true,
		},
		{
			main:     1,
			zero:     0,
			wantMain: 2,
			wantZero: 0,
		},
		{
			main:     2,
			zero:     1,
			wantMain: 2,
			wantZero: 0,
		},
	} {
		t.Run(fmt.Sprintf("main=%d zero=%d", tt.main, tt.zero), func(t *testing.T) {
			c := testConfig()
			snapshot := Snapshot{Pools: map[string]PoolState{}}

			for name, count := range map[string]int{c.MainPool: tt.main, c.ZeroPool: tt.zero} {
				pool := PoolState{
					Capacity:          count,
					Max:               2,
					ProvisioningState: "Succeeded",
					Instances:         map[string]Instance{},
				}

				for i := range count {
					node := testNode(c)
					node.Name = fmt.Sprintf("%s-%d", name, i)
					node.Spec.ProviderID = fmt.Sprintf("azure://%s/virtualMachines/%d", c.PoolID(name), i)
					if name == c.ZeroPool {
						node.Labels[PoolLabel] = ZeroLabel
					}

					snapshot.Nodes = append(snapshot.Nodes, node)
					id := normalizeID(node.Spec.ProviderID)
					pool.Instances[id] = Instance{ID: id}
				}

				snapshot.Pools[name] = pool
			}

			if err := snapshot.Stable(c, tt.wantMain, tt.wantZero); (err == nil) != tt.valid {
				t.Fatalf("Stable error=%v, valid=%v", err, tt.valid)
			}
		})
	}
}

func testNode(c Config) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Labels: map[string]string{PoolLabel: MainLabel}},
		Spec:       corev1.NodeSpec{ProviderID: "azure://" + c.PoolID(c.MainPool) + "/virtualMachines/0"},
		Status: corev1.NodeStatus{
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
		},
	}
}

func testSnapshot(c Config) Snapshot {
	node := testNode(c)
	id := normalizeID(node.Spec.ProviderID)

	return Snapshot{
		Pools: map[string]PoolState{
			c.MainPool: {
				Capacity:          1,
				Max:               2,
				ProvisioningState: "Succeeded",
				Instances:         map[string]Instance{id: {ID: id, NICs: []string{c.PoolID(c.MainPool) + "/virtualMachines/0/networkInterfaces/nic"}}},
			},
			c.ZeroPool: {
				Capacity:          0,
				Max:               1,
				ProvisioningState: "Succeeded",
				Instances:         map[string]Instance{},
			},
		},
		Nodes: []corev1.Node{node, {ObjectMeta: metav1.ObjectMeta{Name: "system-0"},
			Spec: corev1.NodeSpec{ProviderID: "azure://" + c.PoolID("system") + "/virtualMachines/0"}}},
	}
}

func TestSnapshotStable(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		change func(*Snapshot)
		ready  bool
	}{
		{name: "unready worker", change: func(s *Snapshot) { s.Nodes[0].Status.Conditions[0].Status = corev1.ConditionFalse }},
		{name: "cordoned worker", change: func(s *Snapshot) { s.Nodes[0].Spec.Unschedulable = true }},
		{name: "wrong pool label", change: func(s *Snapshot) { s.Nodes[0].Labels["acceptance-pool"] = "wrong" }},
		{name: "capacity without actual VM", change: func(s *Snapshot) { delete(s.Pools["main"].Instances, normalizeID(s.Nodes[0].Spec.ProviderID)) }},
		{name: "stale provider ID", change: func(s *Snapshot) { s.Nodes[0].Spec.ProviderID += "different" }},
		{name: "missing pool", change: func(s *Snapshot) { delete(s.Pools, "zero") }},
		{name: "deleting VM", change: func(s *Snapshot) {
			id := normalizeID(s.Nodes[0].Spec.ProviderID)
			instance := s.Pools["main"].Instances[id]
			instance.ProvisioningState = "Deleting"
			s.Pools["main"].Instances[id] = instance
		}},
		{
			name: "VMSS updating",
			change: func(s *Snapshot) {
				pool := s.Pools["zero"]
				pool.ProvisioningState = "Updating"
				s.Pools["zero"] = pool
			},
			ready: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig()
			s := testSnapshot(c)
			if err := s.Stable(c, 1, 0); err != nil {
				t.Fatalf("a Node of another pool must not count as a worker: %v", err)
			}

			if err := s.Ready(c, 1, 0); err != nil {
				t.Fatalf("valid cloud and Node state rejected: %v", err)
			}

			tt.change(&s)
			if err := s.Stable(c, 1, 0); err == nil {
				t.Fatal("invalid cloud or Node state accepted")
			}

			if err := s.Ready(c, 1, 0); (err == nil) != tt.ready {
				t.Fatalf("Ready returned %v, want ready=%t", err, tt.ready)
			}
		})
	}
}

func TestSnapshotDuplicateRegistration(t *testing.T) {
	t.Parallel()
	c := testConfig()
	s := testSnapshot(c)
	pool := s.Pools[c.MainPool]
	pool.Capacity = 2
	id := normalizeID(c.PoolID(c.MainPool) + "/virtualMachines/1")
	pool.Instances[id] = Instance{ID: id}
	s.Pools[c.MainPool] = pool
	duplicate := *s.Nodes[0].DeepCopy()
	duplicate.Name = "duplicate"
	s.Nodes = append(s.Nodes, duplicate)
	if err := s.Stable(c, 2, 0); err == nil {
		t.Fatal("two Nodes registered to the same VM were accepted")
	}
}

func TestSnapshotRunningStable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Snapshot)
		valid  bool
	}{
		{name: "parked Node retained", valid: true},
		{name: "missing taint", change: func(s *Snapshot) { s.Nodes[2].Spec.Taints = nil }},
		{name: "resume still in flight", change: func(s *Snapshot) { s.Nodes[2].Annotations["cluster-autoscaler.kubernetes.io/azure-resume"] = "{}" }},
		{name: "physical count mismatch", change: func(s *Snapshot) { p := s.Pools["main"]; p.Capacity++; s.Pools["main"] = p }},
		{name: "parked Node missing", change: func(s *Snapshot) { s.Nodes = s.Nodes[:2] }},
		{name: "external Start", change: func(s *Snapshot) {
			id := normalizeID(s.Nodes[2].Spec.ProviderID)
			vm := s.Pools["main"].Instances[id]
			vm.PowerState = "running"
			s.Pools["main"].Instances[id] = vm
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig()
			snapshot := testSnapshot(c)
			node := testNode(c)
			node.Name = "parked"
			node.Spec.ProviderID = strings.TrimSuffix(node.Spec.ProviderID, "0") + "1"
			node.Spec.Taints = []corev1.Taint{{Key: "status-taint.cluster-autoscaler.kubernetes.io/azure-suspended", Effect: corev1.TaintEffectNoSchedule}}
			node.Status.Conditions = []corev1.NodeCondition{{Type: "Suspended", Status: corev1.ConditionTrue}}
			node.Annotations = map[string]string{}
			snapshot.Nodes = append(snapshot.Nodes, node)
			id := normalizeID(node.Spec.ProviderID)
			snapshot.Pools[c.MainPool].Instances[id] = Instance{ID: id, PowerState: "deallocated", ProvisioningState: "Succeeded"}
			pool := snapshot.Pools[c.MainPool]
			pool.Capacity++
			snapshot.Pools[c.MainPool] = pool
			if tc.change != nil {
				tc.change(&snapshot)
			}
			if err := snapshot.RunningStable(c, 1, 0); (err == nil) != tc.valid {
				t.Fatalf("RunningStable()=%v, valid=%t", err, tc.valid)
			}
			if len(snapshot.Pools[c.MainPool].Instances) != 2 {
				t.Fatal("RunningStable mutated the original snapshot")
			}
		})
	}
}
func TestCheckStatusCapturedRuntimePayload(t *testing.T) {
	t.Parallel()
	payload, err := os.ReadFile("testdata/autoscaler-status.yaml")
	if err != nil {
		t.Fatal(err)
	}

	status := string(payload)
	const capturedTime = "2026-09-16 09:21:08.356413974 +0000 UTC"
	now := time.Date(2026, 9, 16, 9, 21, 9, 0, time.UTC)

	for _, tt := range []struct {
		name, status string
		now          time.Time
		podCreated   time.Time
		valid        bool
	}{
		{
			name:   "actual nanosecond timestamp",
			status: status,
			now:    now,
			valid:  true,
		},
		{
			name:   "no fractional seconds",
			status: strings.ReplaceAll(status, capturedTime, "2026-09-16 09:21:08 +0000 UTC"),
			now:    now,
			valid:  true,
		},
		{
			name:   "millisecond timestamp",
			status: strings.ReplaceAll(status, capturedTime, "2026-09-16 09:21:08.356 +0000 UTC"),
			now:    now,
			valid:  true,
		},
		{
			name:   "extra group",
			status: strings.Replace(status, "nodeGroups:\n", "nodeGroups:\n- name: unowned\n", 1),
			now:    now,
		},
		{
			name:       "previous controller status",
			status:     status,
			now:        now,
			podCreated: now,
		},
		{
			name:       "current controller status",
			status:     status,
			now:        now,
			podCreated: now.Add(-time.Second),
			valid:      true,
		},
		{
			name:   "stale captured payload",
			status: status,
			now:    now.Add(3 * time.Minute),
		},
		{
			name:   "future captured payload",
			status: status,
			now:    now.Add(-time.Minute),
		},
		{
			name:   "malformed timestamp",
			status: strings.ReplaceAll(status, capturedTime, "not-a-timestamp"),
			now:    now,
		},
		{
			name:   "wrong inventory",
			status: strings.ReplaceAll(status, "name: zero", "name: foreign"),
			now:    now,
		},
		{
			name:   "inactive autoscaler",
			status: strings.ReplaceAll(status, "autoscalerStatus: Running", "autoscalerStatus: Inactive"),
			now:    now,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := CheckStatus(tt.status, []string{"main", "zero"}, tt.now, tt.podCreated); (err == nil) != tt.valid {
				t.Fatalf("captured status error=%v, valid=%v", err, tt.valid)
			}
		})
	}
}
