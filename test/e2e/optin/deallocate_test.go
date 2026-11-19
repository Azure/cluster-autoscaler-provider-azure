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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

func parkedFixture() (utils.Config, utils.Snapshot, corev1.Node) {
	c := utils.Config{SubscriptionID: "subscription", ResourceGroup: "nodes", MainPool: "main"}
	id := strings.ToLower(c.PoolID(c.MainPool) + "/virtualMachines/1")
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "parked", UID: "retained", Annotations: map[string]string{disabledAnnotation: "true"}},
		Spec: corev1.NodeSpec{ProviderID: "azure://" + id, Taints: []corev1.Taint{{
			Key: suspendedTaint, Effect: corev1.TaintEffectNoSchedule,
		}}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: suspendedCondition, Status: corev1.ConditionTrue}}},
	}
	running := *node.DeepCopy()
	running.Name, running.UID = "running", "running"
	running.Spec.ProviderID = "azure://" + strings.TrimSuffix(id, "1") + "0"
	running.Spec.Taints = nil
	running.Annotations = nil
	running.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	snapshot := utils.Snapshot{
		Pools: map[string]utils.PoolState{c.MainPool: {
			Capacity: 2, Min: 1, Max: 3,
			Instances: map[string]utils.Instance{
				id:              {ID: id, VMID: "parked-vm", PowerState: "deallocated", ProvisioningState: "Succeeded"},
				nodeID(running): {ID: nodeID(running), VMID: "running-vm", PowerState: "running", ProvisioningState: "Succeeded"},
			},
		}},
		Nodes: []corev1.Node{running, node},
	}
	return c, snapshot, node
}

func TestCheckResumeMarker(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*resumeMarker)
		valid  bool
	}{
		{name: "resume", valid: true},
		{name: "repark", change: func(m *resumeMarker) { m.Repark = true }, valid: true},
		{name: "wrong VM", change: func(m *resumeMarker) { m.VMID = "replacement" }},
		{name: "wrong Node", change: func(m *resumeMarker) { m.NodeUID = "replacement" }},
		{name: "missing deadline origin", change: func(m *resumeMarker) { m.Started = time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, snapshot, node := parkedFixture()
			vm := snapshot.Pools["main"].Instances[nodeID(node)]
			marker := resumeMarker{VMID: vm.VMID, NodeUID: node.UID, Started: time.Now().UTC()}
			if tc.change != nil {
				tc.change(&marker)
			}
			value, err := json.Marshal(marker)
			if err != nil {
				t.Fatal(err)
			}
			node.Annotations[resumeAnnotation] = string(value)
			if _, err := checkResumeMarker(node, vm); (err == nil) != tc.valid {
				t.Fatalf("checkResumeMarker()=%v, valid=%t", err, tc.valid)
			}
			node.Annotations[resumeAnnotation] = "{"
			if _, err := checkResumeMarker(node, vm); err == nil {
				t.Fatal("malformed marker accepted")
			}
		})
	}
}

func TestCheckCapacity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*utils.Snapshot)
		valid  bool
	}{
		{name: "retained", valid: true},
		{name: "growth", change: func(s *utils.Snapshot) { p := s.Pools["main"]; p.Capacity++; s.Pools["main"] = p }},
		{name: "new VM", change: func(s *utils.Snapshot) { s.Pools["main"].Instances["new"] = utils.Instance{VMID: "new"} }},
		{name: "replaced VM", change: func(s *utils.Snapshot) {
			id := nodeID(s.Nodes[1])
			vm := s.Pools["main"].Instances[id]
			vm.VMID = "replacement"
			s.Pools["main"].Instances[id] = vm
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, before, _ := parkedFixture()
			_, after, _ := parkedFixture()
			if tc.change != nil {
				tc.change(&after)
			}
			if err := checkCapacity(before, after, c.MainPool); (err == nil) != tc.valid {
				t.Fatalf("checkCapacity()=%v, valid=%t", err, tc.valid)
			}
		})
	}
}

func TestCheckParkedPool(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*utils.Snapshot)
		valid  bool
	}{
		{name: "retained and suspended", valid: true},
		{name: "missing taint", change: func(s *utils.Snapshot) { s.Nodes[1].Spec.Taints = nil }},
		{name: "missing condition", change: func(s *utils.Snapshot) { s.Nodes[1].Status.Conditions = nil }},
		{name: "missing scale-down protection", change: func(s *utils.Snapshot) { s.Nodes[1].Annotations = nil }},
		{name: "resume still in flight", change: func(s *utils.Snapshot) { s.Nodes[1].Annotations[resumeAnnotation] = "{}" }},
		{name: "replaced Node", change: func(s *utils.Snapshot) { s.Nodes[1].UID = "replacement" }},
		{name: "deleted Node", change: func(s *utils.Snapshot) { s.Nodes = s.Nodes[:1] }},
		{name: "external Start", change: func(s *utils.Snapshot) {
			id := nodeID(s.Nodes[1])
			vm := s.Pools["main"].Instances[id]
			vm.PowerState = "starting"
			s.Pools["main"].Instances[id] = vm
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, before, _ := parkedFixture()
			_, after, _ := parkedFixture()
			if tc.change != nil {
				tc.change(&after)
			}
			if err := checkParkedPool(before, after, c, 1); (err == nil) != tc.valid {
				t.Fatalf("checkParkedPool()=%v, valid=%t", err, tc.valid)
			}
		})
	}
}

func TestGroupBackedOff(t *testing.T) {
	t.Parallel()
	since := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	status := "nodeGroups:\n- name: main\n  scaleUp:\n    status: Backoff\n    lastProbeTime: '2026-10-24T00:00:10Z'\n"
	for _, tc := range []struct {
		name, status string
		backoff      bool
		wantErr      bool
	}{
		{name: "matching group", status: status, backoff: true},
		{name: "case-insensitive VMSS name", status: strings.ReplaceAll(status, "main", "MAIN"), backoff: true},
		{name: "another group", status: strings.ReplaceAll(status, "main", "other"), wantErr: true},
		{name: "stale probe", status: strings.ReplaceAll(status, "00:00:10", "00:00:00")},
		{name: "not backed off", status: strings.ReplaceAll(status, "Backoff", "NoActivity")},
		{name: "malformed status", status: "[", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := groupBackedOff(tc.status, "main", since)
			if got != tc.backoff || (err != nil) != tc.wantErr {
				t.Fatalf("groupBackedOff()=(%t,%v), want (%t,error=%t)", got, err, tc.backoff, tc.wantErr)
			}
		})
	}
}

func TestCheckResumeProtection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		suspended bool
		ready     bool
		power     string
		provision string
		valid     bool
	}{
		{name: "in flight", suspended: true, power: "starting", valid: true},
		{name: "status cleared before metadata", ready: true, power: "running", provision: "Succeeded", valid: true},
		{name: "premature status clearing", power: "starting"},
		{name: "VM still provisioning", ready: true, power: "running", provision: "Updating"},
		{name: "Node not Ready", power: "running", provision: "Succeeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, node := parkedFixture()
			node.Status.Conditions = nil
			if tc.suspended {
				node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: suspendedCondition, Status: corev1.ConditionTrue})
			}
			if tc.ready {
				node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue})
			}
			vm := utils.Instance{PowerState: tc.power, ProvisioningState: tc.provision}
			if err := checkResumeProtection(node, vm); (err == nil) != tc.valid {
				t.Fatalf("checkResumeProtection()=%v, valid=%t", err, tc.valid)
			}
			node.Spec.Taints = nil
			if err := checkResumeProtection(node, vm); err == nil {
				t.Fatal("unprotected resume accepted")
			}
		})
	}
}
