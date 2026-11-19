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
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeCloud struct {
	snapshot Snapshot
	exists   bool
	err      error
}

func (f fakeCloud) Read(context.Context) (Snapshot, error) { return f.snapshot, f.err }
func (f fakeCloud) NICExists(context.Context, string) (bool, error) {
	return f.exists, f.err
}

func TestEnvironmentRead(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		change func(*Snapshot)
		cloud  error
		bounds bool
	}{
		{name: "within max tags"},
		{
			name:   "desired capacity above max",
			bounds: true,
			change: func(s *Snapshot) {
				pool := s.Pools["zero"]
				pool.Capacity = 2
				s.Pools["zero"] = pool
			},
		},
		{
			name:   "VMs above max",
			bounds: true,
			change: func(s *Snapshot) {
				s.Pools["main"].Instances["extra-1"] = Instance{}
				s.Pools["main"].Instances["extra-2"] = Instance{}
			},
		},
		{name: "ordinary cloud read failure", cloud: errors.New("temporary failure")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig()
			snapshot := testSnapshot(c)
			snapshot.Nodes = nil
			if tt.change != nil {
				tt.change(&snapshot)
			}

			node := testNode(c)
			e := &Environment{
				Config: c,
				K8s:    fake.NewClientBuilder().WithObjects(&node).Build(),
				Cloud:  fakeCloud{snapshot: snapshot, err: tt.cloud},
			}
			result, err := e.Read(context.Background())
			if errors.Is(err, ErrBounds) != tt.bounds || (err == nil) != (!tt.bounds && tt.cloud == nil) {
				t.Fatalf("Read error=%v, bounds=%t", err, tt.bounds)
			}

			if err == nil && len(result.Nodes) != 1 {
				t.Fatalf("Read returned %d Nodes, want 1", len(result.Nodes))
			}
		})
	}
}

func TestEnvironmentDeleted(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		cloud   fakeCloud
		node    bool
		removed bool
		valid   bool
	}{
		{
			name:    "physical deletion",
			removed: true,
			valid:   true,
		},
		{name: "desired capacity only"},
		{
			name:    "NIC still present",
			removed: true,
			cloud:   fakeCloud{exists: true},
		},
		{
			name:    "Node still present",
			removed: true,
			node:    true,
		},
		{
			name:    "NIC authorization error",
			removed: true,
			cloud:   fakeCloud{err: errors.New("forbidden")},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig()
			before, after := testSnapshot(c), testSnapshot(c)
			if tt.removed {
				after.Pools[c.MainPool] = PoolState{Instances: map[string]Instance{}}
			}

			if !tt.node {
				after.Nodes = nil
			}

			e := &Environment{Config: c, Cloud: tt.cloud}
			if err := e.Deleted(context.Background(), before, after, c.MainPool, 1); (err == nil) != tt.valid {
				t.Fatalf("Deleted = %v, valid=%v", err, tt.valid)
			}
		})
	}
}

func TestEnvironmentDeletedEveryCapturedInstance(t *testing.T) {
	t.Parallel()
	c := testConfig()
	first := normalizeID(c.PoolID(c.ZeroPool) + "/virtualMachines/0")
	replacement := normalizeID(c.PoolID(c.ZeroPool) + "/virtualMachines/1")
	before := Snapshot{Pools: map[string]PoolState{c.ZeroPool: {Instances: map[string]Instance{
		first:       {ID: first, NICs: []string{first + "/networkInterfaces/first"}},
		replacement: {ID: replacement, NICs: []string{replacement + "/networkInterfaces/replacement"}},
	}}}}
	after := Snapshot{Pools: map[string]PoolState{c.ZeroPool: {Instances: map[string]Instance{}}}}
	e := &Environment{Cloud: fakeCloud{}}
	if err := e.Deleted(context.Background(), before, after, c.ZeroPool, 2); err != nil {
		t.Fatal(err)
	}

	after.Pools[c.ZeroPool].Instances[replacement] = before.Pools[c.ZeroPool].Instances[replacement]
	if err := e.Deleted(context.Background(), before, after, c.ZeroPool, 2); err == nil {
		t.Fatal("accepted a replacement VM that still exists")
	}
}

func TestWorkloadStateRequiresSchedulingEvidence(t *testing.T) {
	t.Parallel()
	c := testConfig()
	node := testNode(c)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "demand",
		Namespace: "test",
		Labels:    map[string]string{RunLabel: c.RunID, "app": "demand"},
	},
		Status: corev1.PodStatus{Phase: corev1.PodPending}}
	e := &Environment{Config: c, K8s: fake.NewClientBuilder().WithObjects(&node, pod).Build()}
	if _, err := e.WorkloadState(context.Background(), "test", "demand", c.MainPool, 0, 1); err == nil {
		t.Fatal("ordinary Pending accepted as scheduler rejection")
	}

	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodScheduled,
		Status: corev1.ConditionFalse,
		Reason: corev1.PodReasonUnschedulable,
	}}
	if err := e.K8s.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}

	if _, err := e.WorkloadState(context.Background(), "test", "demand", c.MainPool, 0, 1); err != nil {
		t.Fatal(err)
	}
}
