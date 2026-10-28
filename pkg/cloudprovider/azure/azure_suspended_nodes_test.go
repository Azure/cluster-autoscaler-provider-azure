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

package azure

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config/dynamic"
	coreoptions "sigs.k8s.io/cluster-autoscaler/pkg/core/options"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
	catest "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

func TestSuspendedSetCondition(t *testing.T) {
	node := &apiv1.Node{}
	parked := metav1.NewTime(time.Unix(100, 0))
	require.True(t, setSuspendedCondition(node, apiv1.ConditionTrue, parkedReason, parked))
	require.False(t, setSuspendedCondition(node, apiv1.ConditionTrue, parkedReason, metav1.NewTime(time.Unix(200, 0))))
	require.True(t, parked.Equal(&nodeSuspended(node).LastTransitionTime))
	resumed := metav1.NewTime(time.Unix(300, 0))
	require.True(t, setSuspendedCondition(node, apiv1.ConditionFalse, resumedReason, resumed))
	require.Len(t, node.Status.Conditions, 1)
	require.True(t, resumed.Equal(&nodeSuspended(node).LastTransitionTime))
}

func TestSuspendedAnnotationOwnership(t *testing.T) {
	for _, operatorOwned := range []bool{false, true} {
		t.Run(fmt.Sprint(operatorOwned), func(t *testing.T) {
			node := suspendedTestNode(0, false)
			if operatorOwned {
				node.Annotations = map[string]string{scaleDownDisabledAnnotation: "true"}
			}
			client := fake.NewClientset(node)
			_, group := newSuspendedProvider(t, &suspendedWorld{states: []string{vmPowerStateRunning}}, client, 0, 3)
			require.NoError(t, group.setNodeSuspended(t.Context(), node, true))
			require.NoError(t, group.setNodeSuspended(t.Context(), node, true))
			require.NoError(t, group.setNodeSuspended(t.Context(), node, false))
			current := getTestNode(t, client, node.Name)
			require.Equal(t, operatorOwned, current.Annotations[scaleDownDisabledAnnotation] == "true")
			require.NotContains(t, current.Annotations, "cluster-autoscaler.kubernetes.io/azure-owns-scale-down-disabled")
		})
	}
}

func TestSuspendedRefreshSkipsBusyGroup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, group := newSuspendedProvider(t, &suspendedWorld{states: []string{vmPowerStateRunning}}, fake.NewClientset(), 0, 3)
		group.parkMutex.Lock()
		defer group.parkMutex.Unlock()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.NoError(t, group.reconcileSuspendedNodes(ctx, func() (*apiv1.NodeList, error) {
			t.Fatal("busy group must be skipped")
			return nil, nil
		}))
		require.NoError(t, ctx.Err())
	})
}

func TestSuspendedUnconfirmedDeallocatedPark(t *testing.T) {
	node := suspendedTestNode(0, false)
	world := &suspendedWorld{states: []string{vmPowerStateRunning}, parkError: map[int]error{0: context.DeadlineExceeded}}
	provider, group := newSuspendedProvider(t, world, fake.NewClientset(node), 0, 3)
	require.Error(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}))
	world.setPowerState(0, vmPowerStateDeallocated)
	world.provisioning = map[int]string{0: VMProvisioningStateFailed}
	group.invalidateSuspendedInventory()
	require.NoError(t, provider.Refresh(t.Context()))
	_, pending := group.powerOperation("vm-0")
	require.False(t, pending)
	require.Zero(t, group.parkedCount())
}

func TestSuspendedUpdatingParkKeepsRunningMinimum(t *testing.T) {
	nodes := []*apiv1.Node{suspendedTestNode(0, false), suspendedTestNode(1, false)}
	world := &suspendedWorld{
		states:    []string{vmPowerStateRunning, vmPowerStateRunning},
		parkError: map[int]error{0: context.DeadlineExceeded},
	}
	provider, group := newSuspendedProvider(t, world, fake.NewClientset(nodes[0], nodes[1]), 1, 2)
	require.Error(t, group.DeleteNodes(t.Context(), nodes[:1]))
	world.setPowerState(0, vmPowerStateDeallocated)
	world.provisioning = map[int]string{0: "Updating"}
	group.invalidateSuspendedInventory()
	require.NoError(t, provider.Refresh(t.Context()))
	require.ErrorContains(t, group.DeleteNodes(t.Context(), nodes[1:]), "min size reached")
	_, pending := group.powerOperation("vm-0")
	require.True(t, pending)
	require.Equal(t, 1, group.parkedCount())
	require.Equal(t, []string{"deallocate:0"}, world.history())
}

func TestSuspendedCompletedParkProvidesHeadroom(t *testing.T) {
	node := suspendedTestNode(0, false)
	client := fake.NewClientset(node)
	world := &suspendedWorld{
		states:     []string{vmPowerStateRunning, vmPowerStateRunning},
		parkResult: func(int) error { return nil },
	}
	_, group := newSuspendedProvider(t, world, client, 0, 2)
	client.PrependReactor("update", "nodes", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("metadata unavailable")
	})
	require.ErrorContains(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}), "metadata unavailable")
	op, pending := group.powerOperation("vm-0")
	require.True(t, pending)
	require.True(t, op.completed)
	require.NoError(t, group.IncreaseSize(t.Context(), 1))
	require.Equal(t, []string{"deallocate:0", "grow:3"}, world.history())
}

func TestSuspendedUnconfirmedParkSettlesFromInventory(t *testing.T) {
	node := suspendedTestNode(0, false)
	client := fake.NewClientset(node)
	world := &suspendedWorld{states: []string{vmPowerStateRunning}, parkError: map[int]error{0: context.DeadlineExceeded}}
	provider, group := newSuspendedProvider(t, world, client, 0, 3)
	require.Error(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}))
	world.setPowerState(0, vmPowerStateDeallocated)
	group.invalidateSuspendedInventory()
	require.NoError(t, provider.Refresh(t.Context()))
	_, pending := group.powerOperation("vm-0")
	require.False(t, pending)
	require.True(t, hasSuspendedMetadata(getTestNode(t, client, node.Name)))
}

func TestSuspendedFailedParkKeepsOtherHeadroom(t *testing.T) {
	node := suspendedTestNode(0, false)
	world := &suspendedWorld{
		states:     []string{vmPowerStateRunning, vmPowerStateDeallocated, vmPowerStateRunning},
		parkResult: func(int) error { return &azcore.ResponseError{StatusCode: http.StatusOK} },
	}
	_, group := newSuspendedProvider(t, world, fake.NewClientset(node), 0, 3)
	require.Error(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}))
	world.provisioning = map[int]string{0: "Updating"}
	require.NoError(t, group.IncreaseSize(t.Context(), 1))
	require.Equal(t, []string{"deallocate:0", "grow:4"}, world.history())
}

func TestSuspendedGrowthIgnoresTransitionalPower(t *testing.T) {
	for _, state := range []string{vmPowerStateDeallocating, vmPowerStateStopping} {
		t.Run(state, func(t *testing.T) {
			world := &suspendedWorld{states: []string{state}}
			_, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			require.NoError(t, group.IncreaseSize(t.Context(), 1))
			require.Equal(t, []string{"grow:2"}, world.history())
		})
	}
}

func TestSuspendedConfigureAutoscaler(t *testing.T) {
	manager := &AzureManager{}
	opts := &coreoptions.AutoscalerOptions{}
	opts.CordonNodeBeforeTerminate = true
	manager.configureAutoscaler(opts)
	require.True(t, manager.cordonNodeBeforeTerminate)
}

func TestSuspendedParkPreservesNodeAndInventory(t *testing.T) {
	node := suspendedTestNode(0, false)
	custom := apiv1.Taint{Key: "example.com/keep", Effect: apiv1.TaintEffectPreferNoSchedule}
	node.Spec.Taints = []apiv1.Taint{custom,
		{Key: taints.ToBeDeletedTaint, Effect: apiv1.TaintEffectNoSchedule},
		{Key: taints.DeletionCandidateTaintKey, Effect: apiv1.TaintEffectPreferNoSchedule}}
	node.Spec.Unschedulable = true
	client := fake.NewClientset(node)
	world := &suspendedWorld{states: []string{vmPowerStateRunning}}
	provider, group := newSuspendedProvider(t, world, client, 0, 3)
	require.NoError(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}))
	parked := getTestNode(t, client, node.Name)
	require.Equal(t, node.UID, parked.UID)
	require.Equal(t, node.Spec.ProviderID, parked.Spec.ProviderID)
	require.Equal(t, node.Spec.PodCIDR, parked.Spec.PodCIDR)
	require.Equal(t, node.Labels, parked.Labels)
	require.True(t, hasSuspendedMetadata(parked))
	require.ElementsMatch(t, []apiv1.Taint{custom, {Key: suspendedTaintKey, Effect: apiv1.TaintEffectNoSchedule}}, parked.Spec.Taints)
	require.False(t, parked.Spec.Unschedulable)
	hasInstance, err := provider.HasInstance(t.Context(), parked)
	require.NoError(t, err)
	require.True(t, hasInstance)
	instances, err := group.Nodes(t.Context())
	require.NoError(t, err)
	require.Equal(t, cloudprovider.InstanceRunning, instances[0].Status.State)
	require.NoError(t, group.ForceDeleteNodes(t.Context(), []*apiv1.Node{parked}))
	require.Equal(t, []string{"deallocate:0"}, world.history())
}

func TestSuspendedRefreshMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, power, provisioning string
		suspended, pending, park  bool
		deleteMode, failUpdate    bool
		mutate                    func(*apiv1.Node)
		want                      apiv1.ConditionStatus
	}{
		{name: "park", power: vmPowerStateDeallocated, want: apiv1.ConditionTrue},
		{name: "complete park", power: vmPowerStateDeallocated, suspended: true, want: apiv1.ConditionTrue},
		{name: "externally started", power: vmPowerStateRunning, suspended: true, want: apiv1.ConditionFalse},
		{name: "ordinary running", power: vmPowerStateRunning},
		{name: "running updating", power: vmPowerStateRunning, provisioning: "Updating", suspended: true, want: apiv1.ConditionTrue},
		{name: "pending park", power: vmPowerStateDeallocated, pending: true, park: true},
		{name: "starting", power: vmPowerStateStarting, suspended: true, want: apiv1.ConditionTrue},
		{name: "deallocating", power: vmPowerStateDeallocating},
		{name: "failed", power: vmPowerStateDeallocated, provisioning: VMProvisioningStateFailed},
		{name: "Delete unchanged", power: vmPowerStateDeallocated, deleteMode: true},
		{name: "condition missing", power: vmPowerStateDeallocated, suspended: true, want: apiv1.ConditionTrue,
			mutate: func(n *apiv1.Node) { n.Status.Conditions = nil }},
		{name: "partial park retries", power: vmPowerStateDeallocated, suspended: true, failUpdate: true, want: apiv1.ConditionTrue,
			mutate: func(n *apiv1.Node) { n.Spec.Taints, n.Annotations = nil, nil }},
		{name: "partial clearing retries", power: vmPowerStateRunning, suspended: true, failUpdate: true, want: apiv1.ConditionFalse,
			mutate: func(n *apiv1.Node) { setSuspendedCondition(n, apiv1.ConditionFalse, resumedReason, metav1.Now()) }},
		{name: "wrong taint", power: vmPowerStateDeallocated, suspended: true, want: apiv1.ConditionTrue,
			mutate: func(n *apiv1.Node) { n.Spec.Taints[0].Effect = apiv1.TaintEffectNoExecute }},
		{name: "duplicate taint", power: vmPowerStateDeallocated, suspended: true, want: apiv1.ConditionTrue,
			mutate: func(n *apiv1.Node) { n.Spec.Taints = append(n.Spec.Taints, n.Spec.Taints[0]) }},
		{name: "deletion taints", power: vmPowerStateDeallocated, suspended: true, want: apiv1.ConditionTrue,
			mutate: func(n *apiv1.Node) {
				n.Spec.Unschedulable = true
				n.Spec.Taints = append(n.Spec.Taints, apiv1.Taint{Key: taints.ToBeDeletedTaint},
					apiv1.Taint{Key: taints.DeletionCandidateTaintKey})
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := suspendedTestNode(0, tc.suspended)
			if tc.mutate != nil {
				tc.mutate(node)
			}
			client := fake.NewClientset(node)
			world := &suspendedWorld{states: []string{tc.power}}
			provider, group := newSuspendedProvider(t, world, client, 0, 3)
			if tc.provisioning != "" {
				world.provisioning = map[int]string{0: tc.provisioning}
				group.invalidateSuspendedInventory()
			}
			group.deallocate = !tc.deleteMode
			if tc.pending {
				group.recordPower("vm-0", suspendedPowerOperation{node: node, park: tc.park, polling: tc.park})
			}
			fail := tc.failUpdate
			client.PrependReactor("update", "nodes", func(clienttesting.Action) (bool, runtime.Object, error) {
				if fail {
					return true, nil, errors.New("metadata unavailable")
				}
				return false, nil, nil
			})
			client.ClearActions()
			require.NoError(t, provider.Refresh(t.Context()))
			if tc.name == "complete park" {
				require.Len(t, client.Actions(), 1)
				require.Equal(t, "list", client.Actions()[0].GetVerb())
			}
			fail = false
			require.NoError(t, provider.Refresh(t.Context()))
			current := getTestNode(t, client, node.Name)
			if tc.want == "" {
				require.Equal(t, node, current)
			} else {
				require.Equal(t, tc.want, nodeSuspended(current).Status)
				require.Equal(t, tc.want == apiv1.ConditionTrue, hasSuspendedMetadata(current))
			}
			require.Equal(t, node.UID, current.UID)
			require.Equal(t, node.Spec.PodCIDR, current.Spec.PodCIDR)
			require.False(t, current.Spec.Unschedulable)
			require.Empty(t, world.history())
		})
	}
}

func TestSuspendedRefreshSharesAndLimitsNodeLists(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		worlds := []*suspendedWorld{
			{states: []string{vmPowerStateDeallocated}},
			{name: "pool2", states: []string{vmPowerStateDeallocated}},
		}
		node := suspendedPoolNode(worlds[0], 0, true)
		client := fake.NewClientset(node, suspendedPoolNode(worlds[1], 0, true))
		manager := newSuspendedManager(t, client, worlds...)
		groups := make([]*ScaleSet, len(worlds))
		for i, world := range worlds {
			group, err := NewScaleSet(&dynamic.NodeGroupSpec{Name: world.poolName(), MaxSize: 3}, manager, 1, false)
			require.NoError(t, err)
			group.instancesRefreshPeriod = time.Minute
			manager.RegisterNodeGroup(group)
			groups[i] = group
		}
		manager.azureCache.refreshInterval = time.Hour
		provider := &AzureCloudProvider{azureManager: manager}
		lists := 0
		client.PrependReactor("list", "nodes", func(clienttesting.Action) (bool, runtime.Object, error) {
			lists++
			return false, nil, nil
		})
		require.NoError(t, provider.Refresh(t.Context()))
		require.Equal(t, 1, lists)
		current := getTestNode(t, client, node.Name)
		setSuspendedCondition(current, apiv1.ConditionFalse, resumedReason, metav1.Now())
		_, err := client.CoreV1().Nodes().UpdateStatus(t.Context(), current, metav1.UpdateOptions{})
		require.NoError(t, err)
		groups[0].recordPower("vm-0", suspendedPowerOperation{node: node, park: true, completed: true})
		require.NoError(t, provider.Refresh(t.Context()))
		require.True(t, hasSuspendedMetadata(getTestNode(t, client, node.Name)), "pending reconciliation does not wait for the settled scan")
		for i, delay := range []time.Duration{0, time.Minute - time.Nanosecond, time.Nanosecond, 0} {
			time.Sleep(delay)
			require.NoError(t, provider.Refresh(t.Context()))
			require.Equal(t, []int{1, 1, 2, 2}[i], lists)
		}
	})
}

func TestSuspendedUnconfirmedParkReleasesGroup(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		submitErr   error
		nilPoller   bool
		pollErr     error
	}{
		{name: "uncertain submission", submitErr: context.DeadlineExceeded},
		{name: "uncertain submission while stopped", state: vmPowerStateStopped, submitErr: context.DeadlineExceeded},
		{name: "poll timeout", pollErr: context.DeadlineExceeded},
		{name: "terminal failure", pollErr: &azcore.ResponseError{StatusCode: http.StatusOK}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := suspendedTestNode(0, false)
			client := fake.NewClientset(node)
			state := cmp.Or(tc.state, vmPowerStateRunning)
			world := &suspendedWorld{states: []string{state},
				parkError: map[int]error{0: tc.submitErr}, nilPark: tc.nilPoller,
				parkResult: func(int) error { return tc.pollErr }}
			provider, group := newSuspendedProvider(t, world, client, 0, 3)
			require.Error(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}))
			if tc.submitErr != nil {
				require.Equal(t, 1, group.MinSize(t.Context()))
			}
			if tc.pollErr != nil && isTerminalPowerOperationFailure(tc.pollErr) {
				require.Zero(t, group.parkedCount())
			}

			// The VM is still running or stopped, so Refresh drops the park and growth is no longer refused.
			require.NoError(t, provider.Refresh(t.Context()))
			_, pending := group.powerOperation("vm-0")
			require.False(t, pending)
			require.Zero(t, group.parkedCount())
			require.NoError(t, group.IncreaseSize(t.Context(), 1))
			require.Equal(t, []string{"deallocate:0", "grow:2"}, world.history())
		})
	}
}

func TestSuspendedSpotEviction(t *testing.T) {
	for _, policy := range []armcompute.VirtualMachineEvictionPolicyTypes{
		armcompute.VirtualMachineEvictionPolicyTypesDeallocate, armcompute.VirtualMachineEvictionPolicyTypesDelete,
	} {
		t.Run(string(policy), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				node := suspendedTestNode(1, false)
				catest.SetNodeReadyState(node, false, time.Now())
				client := fake.NewClientset(node)
				world := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateRunning},
					priority: armcompute.VirtualMachinePriorityTypesSpot, eviction: policy}
				provider, group := newSuspendedProvider(t, world, client, 1, 2)
				world.setPowerState(1, vmPowerStateDeallocated)
				if policy == armcompute.VirtualMachineEvictionPolicyTypesDelete {
					world.deleted = map[int]bool{1: true}
					time.Sleep(group.getVmssSizeRefreshPeriod)
					require.True(t, group.suspendedInventoryTime.Add(group.instancesRefreshPeriod).After(time.Now()))
				} else {
					time.Sleep(group.instancesRefreshPeriod)
				}
				require.NoError(t, provider.Refresh(t.Context()))
				require.Empty(t, world.history())
				if policy == armcompute.VirtualMachineEvictionPolicyTypesDelete {
					total, err := group.TargetSize(t.Context())
					require.NoError(t, err)
					require.Equal(t, 1, total)
					instances, err := group.Nodes(t.Context())
					require.NoError(t, err)
					require.Len(t, instances, 1)
					require.Equal(t, node, getTestNode(t, client, node.Name))
					require.NoError(t, group.IncreaseSize(t.Context(), 1))
					require.Equal(t, []string{"grow:2"}, world.history())
				} else {
					require.True(t, hasSuspendedMetadata(getTestNode(t, client, node.Name)))
					require.Equal(t, 2, group.MinSize(t.Context()))
					require.Equal(t, 3, group.MaxSize(t.Context()))
				}
			})
		})
	}
}

func TestSuspendedRunningMinimum(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nodes := []*apiv1.Node{suspendedTestNode(0, false), suspendedTestNode(1, false), suspendedTestNode(2, false)}
		world := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateRunning, vmPowerStateRunning}}
		_, group := newSuspendedProvider(t, world, fake.NewClientset(nodes[0], nodes[1], nodes[2]), 1, 3)
		require.ErrorContains(t, group.DeleteNodes(t.Context(), nodes), "min size reached")
		require.Empty(t, world.history())
		require.NoError(t, group.DeleteNodes(t.Context(), nodes[:2]))
		require.Equal(t, 3, group.MinSize(t.Context()))
		require.Equal(t, 5, group.MaxSize(t.Context()))
		require.ErrorContains(t, group.DeleteNodes(t.Context(), nodes[2:]), "min size reached")
		require.NoError(t, group.DeleteNodes(t.Context(), nodes[:1]))
		require.Equal(t, []string{"deallocate:0", "deallocate:1"}, world.history())
	})
}

func TestSuspendedPendingPark(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nodes := []*apiv1.Node{suspendedTestNode(0, false), suspendedTestNode(1, false)}
		finish := make(chan struct{})
		world := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateRunning},
			parkResult: func(int) error { <-finish; return nil }}
		_, group := newSuspendedProvider(t, world, fake.NewClientset(nodes[0], nodes[1]), 1, 3)
		result := make(chan error, 1)
		defer func() {
			close(finish)
			require.NoError(t, <-result)
		}()
		go func() { result <- group.DeleteNodes(t.Context(), nodes[:1]) }()
		synctest.Wait()
		require.Equal(t, 2, group.MinSize(t.Context()))
		require.Equal(t, 4, group.MaxSize(t.Context()))
		instances, err := group.Nodes(t.Context())
		require.NoError(t, err)
		require.Equal(t, cloudprovider.InstanceCreating, instances[0].Status.State)
		require.ErrorContains(t, group.DeleteNodes(t.Context(), nodes[:1]), "power operation pending")
		require.ErrorContains(t, group.DeleteNodes(t.Context(), nodes[1:]), "min size reached")
		require.ErrorContains(t, group.IncreaseSize(t.Context(), 2), "parking is still in progress")
		require.Equal(t, []string{"deallocate:0"}, world.history())
		require.NoError(t, group.IncreaseSize(t.Context(), 1))
		require.Equal(t, []string{"deallocate:0", "grow:3"}, world.history())
	})
}

func TestSuspendedParkSubmission(t *testing.T) {
	for _, tc := range []struct {
		name      string
		submitErr error
		nilPoller bool
		pollErr   error
		cacheLag  bool
		partial   bool
		failedVM  bool
		retry     bool
	}{
		{name: "rejected", submitErr: &azcore.ResponseError{StatusCode: http.StatusBadRequest}, retry: true},
		{name: "missing poller", nilPoller: true},
		{name: "poll timeout", pollErr: context.DeadlineExceeded},
		{name: "terminal failure", pollErr: &azcore.ResponseError{StatusCode: http.StatusOK}, retry: true},
		{name: "completed with inventory lag", cacheLag: true},
		{name: "partial park", partial: true},
		{name: "registered failed VM", failedVM: true, cacheLag: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := suspendedTestNode(0, false)
			client := fake.NewClientset(node)
			world := &suspendedWorld{states: []string{vmPowerStateRunning},
				parkError: map[int]error{0: tc.submitErr}, nilPark: tc.nilPoller,
				parkResult: func(int) error { return tc.pollErr }}
			nodes := []*apiv1.Node{node}
			if tc.partial {
				second := suspendedTestNode(1, false)
				_, err := client.CoreV1().Nodes().Create(t.Context(), second, metav1.CreateOptions{})
				require.NoError(t, err)
				nodes = append(nodes, second)
				world.states = append(world.states, vmPowerStateRunning)
				world.parkError[1] = &azcore.ResponseError{StatusCode: http.StatusBadRequest}
			}
			if tc.failedVM {
				world.states[0] = vmPowerStateDeallocated
				world.provisioning = map[int]string{0: VMProvisioningStateFailed}
			}
			_, group := newSuspendedProvider(t, world, client, 0, 3)
			err := group.DeleteNodes(t.Context(), nodes)
			if tc.partial {
				require.Error(t, err)
				require.True(t, hasSuspendedMetadata(getTestNode(t, client, node.Name)))
				require.Nil(t, nodeSuspended(getTestNode(t, client, nodes[1].Name)))
				require.Equal(t, []string{"deallocate:0", "deallocate:1"}, world.history())
				return
			}
			if tc.cacheLag {
				require.NoError(t, err)
				require.True(t, hasSuspendedMetadata(getTestNode(t, client, node.Name)))
			} else {
				require.Error(t, err)
				require.Nil(t, nodeSuspended(getTestNode(t, client, node.Name)))
				if tc.retry {
					world.parkError, world.parkResult = nil, nil
					require.NoError(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}))
					require.Equal(t, []string{"deallocate:0", "deallocate:0"}, world.history())
					return
				}
				// Azure parks the VM after all, so it settles without another Deallocate.
				world.setPowerState(0, vmPowerStateDeallocated)
			}
			require.NoError(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}))
			require.True(t, hasSuspendedMetadata(getTestNode(t, client, node.Name)))
			require.Equal(t, []string{"deallocate:0"}, world.history())
		})
	}
}

func TestSuspendedFailingParkAllowsOtherOperations(t *testing.T) {
	node, active := suspendedTestNode(0, false), suspendedTestNode(1, false)
	client := fake.NewClientset(node, active)
	world := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateRunning}}
	_, group := newSuspendedProvider(t, world, client, 0, 2)
	group.recordPower("vm-0", suspendedPowerOperation{node: node, park: true, completed: true})
	client.PrependReactor("update", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.(clienttesting.UpdateAction).GetObject().(*apiv1.Node).Name == node.Name {
			return true, nil, errors.New("metadata unavailable")
		}
		return false, nil, nil
	})
	require.NoError(t, group.DeleteNodes(t.Context(), []*apiv1.Node{active}))
	require.True(t, hasSuspendedMetadata(getTestNode(t, client, active.Name)))
	require.Equal(t, []string{"deallocate:1"}, world.history())
}

func TestSuspendedNodeUpdateRetriesAndFencing(t *testing.T) {
	for _, kind := range []string{"conflict", "completed park replacement", "provider ID replaced", "missing"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				node := suspendedTestNode(0, true)
				client := fake.NewClientset(node)
				world := &suspendedWorld{states: []string{vmPowerStateRunning}}
				provider, group := newSuspendedProvider(t, world, client, 0, 2)
				current := getTestNode(t, client, node.Name)
				switch kind {
				case "UID replaced", "completed park replacement", "provider ID replaced", "missing":
					if kind == "missing" {
						require.NoError(t, client.CoreV1().Nodes().Delete(t.Context(), node.Name, metav1.DeleteOptions{}))
					} else {
						if kind != "provider ID replaced" {
							current.UID = "replacement"
						} else {
							current.Spec.ProviderID += "-replacement"
						}
						_, err := client.CoreV1().Nodes().Update(t.Context(), current, metav1.UpdateOptions{})
						require.NoError(t, err)
						require.ErrorIs(t, group.setNodeSuspended(t.Context(), node, true), errNodeChangedIdentity)
					}
					op := suspendedPowerOperation{node: node, park: true, completed: true}
					group.recordPower("vm-0", op)
					require.NoError(t, provider.Refresh(t.Context()))
					require.Empty(t, group.pendingPower)
					require.Empty(t, world.history())
					if kind != "missing" {
						require.Equal(t, current, getTestNode(t, client, node.Name))
					}
				default:
					calls := 0
					client.PrependReactor("update", "nodes", func(clienttesting.Action) (bool, runtime.Object, error) {
						calls++
						if calls == 1 {
							return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, node.Name, errors.New("conflict"))
						}
						return false, nil, nil
					})
					require.NoError(t, group.setNodeSuspended(t.Context(), node, false))
					require.GreaterOrEqual(t, calls, 2)
					require.False(t, hasSuspendedMetadata(getTestNode(t, client, node.Name)))
				}
			})
		})
	}
}

func TestSuspendedSyntheticCleanup(t *testing.T) {
	for _, reason := range []string{cloudprovider.FakeNodeUnregistered, cloudprovider.FakeNodeCreateError} {
		for _, power := range []string{vmPowerStateRunning, vmPowerStateDeallocated} {
			t.Run(reason+"/"+power, func(t *testing.T) {
				node := suspendedTestNode(0, power == vmPowerStateDeallocated)
				client := fake.NewClientset(node)
				world := &suspendedWorld{states: []string{power}}
				provider, group := newSuspendedProvider(t, world, client, 0, 1)
				synthetic := node.DeepCopy()
				synthetic.UID = ""
				synthetic.Annotations = map[string]string{cloudprovider.FakeNodeReasonAnnotation: reason}
				require.ErrorContains(t, group.ForceDeleteNodes(t.Context(), []*apiv1.Node{synthetic}), "registered instance")
				require.Empty(t, world.history())
				require.NoError(t, client.CoreV1().Nodes().Delete(t.Context(), node.Name, metav1.DeleteOptions{}))
				require.NoError(t, group.ForceDeleteNodes(t.Context(), []*apiv1.Node{synthetic}))
				require.Equal(t, []string{"physical-delete:0"}, world.history())
				hasInstance, err := provider.HasInstance(t.Context(), synthetic)
				require.NoError(t, err)
				require.False(t, hasInstance)
			})
		}
	}
}
