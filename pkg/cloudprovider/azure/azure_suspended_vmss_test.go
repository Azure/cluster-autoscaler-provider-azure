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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v8"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachinescalesetclient/mock_virtualmachinescalesetclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachinescalesetvmclient/mock_virtualmachinescalesetvmclient"
	providerconfig "sigs.k8s.io/cloud-provider-azure/pkg/provider/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/config/dynamic"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodegroupconfig"
	catest "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const suspendedVMSSID = "/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/virtualMachineScaleSets/pool"

type suspendedWorld struct {
	mu           sync.Mutex
	states       []string
	priority     armcompute.VirtualMachinePriorityTypes
	eviction     armcompute.VirtualMachineEvictionPolicyTypes
	identities   map[int]string
	provisioning map[int]string
	deleted      map[int]bool
	events       []string
	startError   map[int]error
	parkError    map[int]error
	nilStart     bool
	nilPark      bool
	startResult  func(int) error
	parkResult   func(int) error
	inventoryErr error
	growthErr    error
}

func (w *suspendedWorld) history() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.events)
}

func (w *suspendedWorld) vmss() *armcompute.VirtualMachineScaleSet {
	w.mu.Lock()
	defer w.mu.Unlock()
	set := newTestVMSSList(int64(len(w.states)-len(w.deleted)), "pool", "eastus", armcompute.OrchestrationModeUniform)[0]
	set.ID = ptr.To(suspendedVMSSID)
	set.Tags = map[string]*string{nodeLabelTagName + "pool": ptr.To("workers")}
	set.Properties.VirtualMachineProfile = &armcompute.VirtualMachineScaleSetVMProfile{
		StorageProfile: &armcompute.VirtualMachineScaleSetStorageProfile{
			OSDisk: &armcompute.VirtualMachineScaleSetOSDisk{
				ManagedDisk: &armcompute.VirtualMachineScaleSetManagedDiskParameters{},
			},
		},
	}

	if w.priority != "" {
		set.Properties.VirtualMachineProfile.Priority = ptr.To(w.priority)
	}

	if w.eviction != "" {
		set.Properties.VirtualMachineProfile.EvictionPolicy = ptr.To(w.eviction)
	}
	return set
}

func (w *suspendedWorld) vms() ([]*armcompute.VirtualMachineScaleSetVM, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	vms := make([]*armcompute.VirtualMachineScaleSetVM, 0, len(w.states))
	for i, state := range w.states {
		if w.deleted[i] {
			continue
		}

		identity := fmt.Sprintf("vm-%d", i)
		if id, ok := w.identities[i]; ok {
			identity = id
		}

		provisioning := provisioningStateSucceeded
		if p, ok := w.provisioning[i]; ok {
			provisioning = p
		}

		vm := &armcompute.VirtualMachineScaleSetVM{
			ID:         ptr.To(fmt.Sprintf("%s/virtualMachines/%d", suspendedVMSSID, i)),
			InstanceID: ptr.To(strconv.Itoa(i)),
			Properties: &armcompute.VirtualMachineScaleSetVMProperties{
				VMID: ptr.To(identity), ProvisioningState: ptr.To(provisioning),
			},
		}
		if state != "" {
			vm.Properties.InstanceView = &armcompute.VirtualMachineScaleSetVMInstanceView{
				Statuses: []*armcompute.InstanceViewStatus{{Code: ptr.To(state)}},
			}
		}

		vms = append(vms, vm)
	}
	return vms, w.inventoryErr
}

type suspendedTestPollingHandler[T any] struct {
	done     bool
	complete func() error
}

func (h *suspendedTestPollingHandler[T]) Done() bool { return h.done }
func (h *suspendedTestPollingHandler[T]) Poll(context.Context) (*http.Response, error) {
	h.done = true
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody}, nil
}
func (h *suspendedTestPollingHandler[T]) Result(context.Context, *T) error {
	return h.complete()
}

func suspendedTestPoller[T any](complete func() error) (*azruntime.Poller[T], error) {
	return azruntime.NewPoller(
		&http.Response{StatusCode: http.StatusAccepted, Header: http.Header{}, Body: http.NoBody},
		azruntime.NewPipeline("test", "v0.0.0", azruntime.PipelineOptions{}, nil),
		&azruntime.NewPollerOptions[T]{Handler: &suspendedTestPollingHandler[T]{complete: complete}},
	)
}

func (w *suspendedWorld) BeginStart(
	_ context.Context, _, _, id string,
	_ *armcompute.VirtualMachineScaleSetVMsClientBeginStartOptions,
) (*azruntime.Poller[armcompute.VirtualMachineScaleSetVMsClientStartResponse], error) {
	i, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}

	w.mu.Lock()
	w.events = append(w.events, "start:"+id)
	err, nilPoller := w.startError[i], w.nilStart
	w.mu.Unlock()
	if err != nil || nilPoller {
		return nil, err
	}
	return suspendedTestPoller[armcompute.VirtualMachineScaleSetVMsClientStartResponse](func() error {
		if w.startResult != nil {
			return w.startResult(i)
		}

		w.mu.Lock()
		defer w.mu.Unlock()
		w.states[i] = vmPowerStateRunning
		return nil
	})
}

func (w *suspendedWorld) BeginDeallocate(
	_ context.Context, _, _, id string,
	_ *armcompute.VirtualMachineScaleSetVMsClientBeginDeallocateOptions,
) (*azruntime.Poller[armcompute.VirtualMachineScaleSetVMsClientDeallocateResponse], error) {
	i, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}

	w.mu.Lock()
	w.events = append(w.events, "deallocate:"+id)
	err, nilPoller := w.parkError[i], w.nilPark
	w.mu.Unlock()
	if err != nil || nilPoller {
		return nil, err
	}
	return suspendedTestPoller[armcompute.VirtualMachineScaleSetVMsClientDeallocateResponse](func() error {
		if w.parkResult != nil {
			return w.parkResult(i)
		}

		w.mu.Lock()
		defer w.mu.Unlock()
		w.states[i] = vmPowerStateDeallocated
		return nil
	})
}

func newSuspendedProvider(
	t *testing.T, world *suspendedWorld, client *fake.Clientset, minimum, maximum int,
) (*AzureCloudProvider, *ScaleSet) {
	t.Helper()
	ctrl := gomock.NewController(t)
	vmssClient := mock_virtualmachinescalesetclient.NewMockInterface(ctrl)
	vmssClient.EXPECT().List(gomock.Any(), "rg").DoAndReturn(
		func(context.Context, string) ([]*armcompute.VirtualMachineScaleSet, error) {
			return []*armcompute.VirtualMachineScaleSet{world.vmss()}, nil
		}).AnyTimes()
	vmssClient.EXPECT().Get(gomock.Any(), "rg", gomock.Any(), nil).DoAndReturn(
		func(_ context.Context, _ string, name string,
			_ *armcompute.ExpandTypesForGetVMScaleSets,
		) (*armcompute.VirtualMachineScaleSet, error) {
			if !strings.EqualFold(name, "pool") {
				return nil, fmt.Errorf("unexpected VMSS %s", name)
			}
			return world.vmss(), nil
		}).AnyTimes()

	vmClient := mock_virtualmachineclient.NewMockInterface(ctrl)
	vmClient.EXPECT().List(gomock.Any(), "rg").Return([]*armcompute.VirtualMachine{}, nil).AnyTimes()

	instanceClient := mock_virtualmachinescalesetvmclient.NewMockInterface(ctrl)
	instanceClient.EXPECT().ListVMInstanceView(gomock.Any(), "rg", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, name string) ([]*armcompute.VirtualMachineScaleSetVM, error) {
			if !strings.EqualFold(name, "pool") {
				return nil, fmt.Errorf("unexpected VMSS %s", name)
			}
			return world.vms()
		}).AnyTimes()

	resizeClient := NewMockVMSSDeleteClient(ctrl)
	resizeClient.EXPECT().BeginCreateOrUpdate(gomock.Any(), "rg", "pool", gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string, vmss armcompute.VirtualMachineScaleSet,
			_ *armcompute.VirtualMachineScaleSetsClientBeginCreateOrUpdateOptions,
		) (*azruntime.Poller[armcompute.VirtualMachineScaleSetsClientCreateOrUpdateResponse], error) {
			world.mu.Lock()
			defer world.mu.Unlock()
			world.events = append(world.events, fmt.Sprintf("grow:%d", *vmss.SKU.Capacity))
			if world.growthErr != nil {
				return nil, world.growthErr
			}

			for len(world.states)-len(world.deleted) < int(*vmss.SKU.Capacity) {
				world.states = append(world.states, vmPowerStateStarting)
			}
			return suspendedTestPoller[armcompute.VirtualMachineScaleSetsClientCreateOrUpdateResponse](func() error { return nil })
		}).AnyTimes()

	resizeClient.EXPECT().BeginDeleteInstances(gomock.Any(), "rg", "pool", gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string, ids armcompute.VirtualMachineScaleSetVMInstanceRequiredIDs,
			_ *armcompute.VirtualMachineScaleSetsClientBeginDeleteInstancesOptions,
		) (*azruntime.Poller[armcompute.VirtualMachineScaleSetsClientDeleteInstancesResponse], error) {
			world.mu.Lock()
			defer world.mu.Unlock()
			if world.deleted == nil {
				world.deleted = make(map[int]bool)
			}

			for _, id := range ids.InstanceIDs {
				i, err := strconv.Atoi(*id)
				if err != nil {
					return nil, err
				}

				world.events = append(world.events, "physical-delete:"+*id)
				world.deleted[i] = true
			}
			return nil, nil
		}).AnyTimes()

	manager := &AzureManager{
		config: &Config{
			Config:     providerconfig.Config{VMType: "vmss", ResourceGroup: "rg", Location: "eastus"},
			Deallocate: true,
		},
		env: azure.PublicCloud, kubeClient: client, cordonNodeBeforeTerminate: true,
		explicitlyConfigured: map[string]bool{"pool": true},
		azClient: &azClient{
			virtualMachineScaleSetsClient: vmssClient, virtualMachineScaleSetVMsClient: instanceClient,
			virtualMachinesClient: vmClient, vmssClientForDelete: resizeClient, vmssPowerClient: world,
		},
	}

	var err error
	manager.azureCache, err = newAzureCache(manager.azClient, time.Minute, *manager.config)
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)

	require.NoError(t, manager.forceRefresh())

	group, err := NewScaleSet(&dynamic.NodeGroupSpec{Name: "pool", MinSize: minimum, MaxSize: maximum},
		manager, *world.vmss().SKU.Capacity, false)
	require.NoError(t, err)
	manager.RegisterNodeGroup(group)

	require.NoError(t, manager.forceRefresh())

	provider, err := BuildAzureCloudProvider(manager, cloudprovider.NewResourceLimiter(map[string]int64{}, map[string]int64{}))
	require.NoError(t, err)
	return provider.(*AzureCloudProvider), group
}

func suspendedTestNode(id int, suspended bool) *apiv1.Node {
	node := catest.BuildTestNode(fmt.Sprintf("node-%d", id), 4000, 8*1024*1024*1024, catest.IsReady(true))
	node.UID = types.UID(fmt.Sprintf("old-%d", id))
	node.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	node.Spec.ProviderID = fmt.Sprintf("%s%s/virtualMachines/%d", azurePrefix, suspendedVMSSID, id)
	node.Spec.PodCIDR = fmt.Sprintf("10.244.%d.0/24", id)
	node.Labels = map[string]string{
		"pool": "workers", apiv1.LabelHostname: node.Name,
		apiv1.LabelOSStable: "linux", apiv1.LabelArchStable: "amd64",
		apiv1.LabelInstanceTypeStable: "Standard_D4_v2", apiv1.LabelTopologyRegion: "eastus",
	}

	if suspended {
		setSuspendedCondition(node, apiv1.ConditionTrue, parkedReason, metav1.NewTime(time.Now().Add(-time.Hour)))
		node.Annotations = map[string]string{scaleDownDisabledAnnotation: "true"}
		node.Spec.Taints = []apiv1.Taint{{Key: suspendedTaintKey, Effect: apiv1.TaintEffectNoSchedule}}
	}
	return node
}

func nodeSuspended(node *apiv1.Node) *apiv1.NodeCondition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == suspendedCondition {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

func TestSuspendedAKSPool(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       *armcontainerservice.ScaleDownMode
		properties bool
		mutate     func(*ScaleSet, *armcompute.VirtualMachineScaleSet)
		getError   error
		wantError  string
		readPool   bool
	}{
		{name: "deallocate", mode: ptr.To(armcontainerservice.ScaleDownModeDeallocate), properties: true, readPool: true},
		{name: "delete", mode: ptr.To(armcontainerservice.ScaleDownModeDelete), properties: true, readPool: true,
			wantError: "Deallocate scale-down mode"},
		{name: "unset mode", properties: true, readPool: true, wantError: "Deallocate scale-down mode"},
		{name: "missing properties", readPool: true, wantError: "Deallocate scale-down mode"},
		{name: "API error", readPool: true, getError: errors.New("access denied"), wantError: "access denied"},

		{name: "missing pool name", mutate: func(_ *ScaleSet, vmss *armcompute.VirtualMachineScaleSet) {
			delete(vmss.Tags, agentpoolNameTag)
		}, wantError: "AKS pool name"},
		{name: "missing cluster name", mutate: func(s *ScaleSet, _ *armcompute.VirtualMachineScaleSet) {
			s.manager.config.ClusterName = ""
		}, wantError: "clusterName"},

		{name: "missing cluster resource group", mutate: func(s *ScaleSet, _ *armcompute.VirtualMachineScaleSet) {
			s.manager.config.ClusterResourceGroup = ""
		}, wantError: "clusterResourceGroup"},

		{name: "missing client", mutate: func(s *ScaleSet, _ *armcompute.VirtualMachineScaleSet) {
			s.manager.azClient.agentPoolClient = nil
		}, wantError: "agent pool client"},

		{name: "hosted subscription without policy", mutate: func(s *ScaleSet, _ *armcompute.VirtualMachineScaleSet) {
			s.manager.config.HostedSubscriptionID = "hosted"
		}, wantError: "explicit Deallocate policy"},
		{name: "hosted resource group without policy", mutate: func(s *ScaleSet, _ *armcompute.VirtualMachineScaleSet) {
			s.manager.config.HostedResourceGroup = "hosted"
		}, wantError: "explicit Deallocate policy"},

		{name: "hosted proxy without policy", mutate: func(s *ScaleSet, _ *armcompute.VirtualMachineScaleSet) {
			s.manager.config.HostedResourceProxyURL = "https://hosted"
		}, wantError: "explicit Deallocate policy"},

		{name: "hosted explicit policy without cluster config", mutate: func(s *ScaleSet, _ *armcompute.VirtualMachineScaleSet) {
			s.manager.config.HostedSubscriptionID = "hosted"
			s.manager.config.ClusterName = ""
			s.manager.config.ClusterResourceGroup = ""
			s.manager.azClient.agentPoolClient = nil
			s.explicitDeallocate = true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := &suspendedWorld{states: []string{vmPowerStateRunning}}
			_, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			client := NewMockAgentPoolsClient(gomock.NewController(t))
			group.manager.azClient.agentPoolClient = client
			group.manager.config.ClusterName = "cluster"
			group.manager.config.ClusterResourceGroup = "cluster-rg"
			vmss, err := group.getVMSSFromCache()
			require.NoError(t, err)

			vmss.Tags["aks-managed-cluster-name"] = ptr.To("cluster")
			vmss.Tags[agentpoolNameTag] = ptr.To("workers")

			if tc.mutate != nil {
				tc.mutate(group, vmss)
			}

			if tc.readPool {
				response := armcontainerservice.AgentPoolsClientGetResponse{}
				if tc.properties {
					response.Properties = &armcontainerservice.ManagedClusterAgentPoolProfileProperties{
						ScaleDownMode: tc.mode,
					}
				}

				client.EXPECT().Get(gomock.Any(), "cluster-rg", "cluster", "workers", nil).Return(response, tc.getError)
			}

			for range 3 {
				_, err = group.suspendedInventory(t.Context(), false)

				if tc.wantError == "" {
					assert.NoError(t, err)
				} else {
					assert.ErrorContains(t, err, tc.wantError)
				}

				if tc.getError != nil {
					assert.ErrorIs(t, err, tc.getError)
				}
			}

			assert.Empty(t, world.history())
		})
	}
}

func TestSuspendedAKSPoolModeRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := &suspendedWorld{states: []string{vmPowerStateRunning}}
		_, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
		manager := group.manager
		manager.config.ClusterName = "cluster"
		manager.config.ClusterResourceGroup = "cluster-rg"
		vmss, err := group.getVMSSFromCache()
		require.NoError(t, err)

		vmss.Tags[agentpoolNameTag] = ptr.To("workers")
		mode := armcontainerservice.ScaleDownModeDeallocate
		calls := 0
		client := NewMockAgentPoolsClient(gomock.NewController(t))
		client.EXPECT().Get(gomock.Any(), "cluster-rg", "cluster", "workers", nil).DoAndReturn(
			func(context.Context, string, string, string, *armcontainerservice.AgentPoolsClientGetOptions) (armcontainerservice.AgentPoolsClientGetResponse, error) {
				calls++
				return armcontainerservice.AgentPoolsClientGetResponse{
					AgentPool: armcontainerservice.AgentPool{
						Properties: &armcontainerservice.ManagedClusterAgentPoolProfileProperties{ScaleDownMode: ptr.To(mode)},
					},
				}, nil
			}).Times(4)
		manager.azClient.agentPoolClient = client
		_, err = group.suspendedInventory(t.Context(), false)
		require.NoError(t, err)
		assert.Equal(t, 1, calls)

		next, err := NewScaleSet(&dynamic.NodeGroupSpec{Name: "pool", MinSize: 0, MaxSize: 4}, manager, 1, false)
		require.NoError(t, err)
		assert.Equal(t, 2, calls, "construction validates the new group once")

		mode = armcontainerservice.ScaleDownModeDelete

		for range 3 {
			_, err = group.suspendedInventory(t.Context(), false)
			require.NoError(t, err)
		}

		time.Sleep(manager.azureCache.refreshInterval - time.Nanosecond)
		_, err = group.suspendedInventory(t.Context(), false)
		require.NoError(t, err)
		assert.Equal(t, 2, calls)
		time.Sleep(time.Nanosecond)
		_, err = group.suspendedInventory(t.Context(), false)
		assert.ErrorContains(t, err, "Deallocate scale-down mode")
		assert.Equal(t, 3, calls)

		mode = armcontainerservice.ScaleDownModeDeallocate

		assert.True(t, manager.RegisterNodeGroup(next))

		for range 3 {
			_, err = next.suspendedInventory(t.Context(), false)
			assert.ErrorContains(t, err, "Deallocate scale-down mode")
		}

		assert.Equal(t, 3, calls, "rediscovery retains the cached failure")
		time.Sleep(manager.azureCache.refreshInterval)
		_, err = next.suspendedInventory(t.Context(), false)
		require.NoError(t, err)
		assert.Equal(t, 4, calls)
		assert.Empty(t, world.history())
	})
}

func TestSuspendedHostedPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, spec string
		global     bool
		wantError  bool
		suspended  bool
	}{
		{name: "selected", spec: "0:3:Deallocate:pool", suspended: true},
		{name: "extended", spec: `0:3:Deallocate:pool:{"env":"prod"}|key=value:NoSchedule`, suspended: true},
		{name: "selected with global", spec: "0:3:Deallocate:pool", global: true, suspended: true},
		{name: "Delete", spec: "0:3:Delete:pool"},
		{name: "unset", spec: "0:3:pool"},
		{name: "global Delete", spec: "0:3:Delete:pool", global: true, wantError: true},
		{name: "global unset", spec: "0:3:pool", global: true, wantError: true},
		{name: "global only", global: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := &suspendedWorld{states: []string{vmPowerStateRunning}}
			provider, original := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			m := provider.azureManager
			m.UnregisterNodeGroup(original)
			m.config.Deallocate = tc.global
			m.config.HostedSubscriptionID = "hosted"
			m.config.HostedResourceGroup = "rg"
			m.config.HostedResourceProxyURL = "https://proxy"
			m.azClient.agentPoolClient = NewMockAgentPoolsClient(gomock.NewController(t))
			vmss := world.vmss()
			vmss.Tags[agentpoolNameTag] = ptr.To("workers")
			m.azureCache.setScaleSet("pool", vmss)
			var group cloudprovider.NodeGroup
			var err error

			if tc.spec == "" {
				group, err = NewScaleSet(&dynamic.NodeGroupSpec{Name: "pool", MaxSize: 3}, m, 1, false)
			} else {
				group, err = m.buildNodeGroupFromSpec(tc.spec)
			}

			if tc.wantError {
				var unsupported *unsupportedDeallocateError
				require.ErrorAs(t, err, &unsupported)
				require.ErrorContains(t, err, "explicit Deallocate policy")
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.suspended, group.(*ScaleSet).suspendsParkedNodes())
			require.Empty(t, world.history())
		})
	}
}

func TestSuspendedSpotPool(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected", true: "global"}[global], func(t *testing.T) {
			world := &suspendedWorld{
				states: []string{vmPowerStateRunning}, priority: armcompute.VirtualMachinePriorityTypesSpot,
				eviction: armcompute.VirtualMachineEvictionPolicyTypesDeallocate,
			}
			provider, original := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			manager := provider.azureManager
			manager.UnregisterNodeGroup(original)
			manager.config.Deallocate = global
			spec := "0:3:Deallocate:pool"
			if global {
				spec = "0:3:Delete:pool"
			}

			require.NoError(t, manager.fetchExplicitNodeGroups([]string{spec}))
			require.Len(t, manager.getNodeGroups(), 1)

			group := manager.getNodeGroups()[0].(*ScaleSet)

			require.True(t, group.suspendsParkedNodes())
			require.NoError(t, group.validateSuspendedMode())
			require.Empty(t, world.history())
		})
	}
}

func TestSuspendedInventoryPowerStates(t *testing.T) {
	for _, tc := range []struct {
		name, power, provisioning string
		wantError                 bool
		wantState                 cloudprovider.InstanceState
		wantFailure               bool
	}{
		{name: "parked", power: vmPowerStateDeallocated, wantState: cloudprovider.InstanceRunning},
		{name: "running", power: vmPowerStateRunning, wantState: cloudprovider.InstanceRunning},
		{name: "missing", wantError: true},
		{name: "unknown", power: vmPowerStateUnknown, wantError: true},
		{name: "starting", power: vmPowerStateStarting, wantState: cloudprovider.InstanceCreating},
		{name: "deallocating", power: vmPowerStateDeallocating, wantState: cloudprovider.InstanceCreating},
		{name: "failed missing power", provisioning: VMProvisioningStateFailed, wantState: cloudprovider.InstanceCreating},
		{name: "failed transitional", power: vmPowerStateStarting, provisioning: VMProvisioningStateFailed, wantState: cloudprovider.InstanceCreating},
		{name: "failed stopped", power: vmPowerStateStopped, provisioning: VMProvisioningStateFailed,
			wantState: cloudprovider.InstanceCreating, wantFailure: true},
		{name: "failed deallocated", power: vmPowerStateDeallocated, provisioning: VMProvisioningStateFailed,
			wantState: cloudprovider.InstanceCreating, wantFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := &suspendedWorld{states: []string{vmPowerStateRunning}, provisioning: map[int]string{}}
			_, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			world.mu.Lock()
			world.states[0] = tc.power

			if tc.provisioning != "" {
				world.provisioning[0] = tc.provisioning
			}

			world.mu.Unlock()
			group.invalidateSuspendedInventory()
			group.enableFastDeleteOnFailedProvisioning = true
			instances, err := group.Nodes(t.Context())

			if tc.wantError {
				require.ErrorContains(t, err, "unknown VM power state")
				require.Empty(t, world.history())
				return
			}

			require.NoError(t, err)
			require.Len(t, instances, 1)
			require.Equal(t, tc.wantState, instances[0].Status.State)
			require.Equal(t, tc.wantFailure, instances[0].Status.ErrorInfo != nil)

			instances[0].Status.State = cloudprovider.InstanceDeleting
			again, err := group.Nodes(t.Context())
			require.NoError(t, err)
			require.Equal(t, tc.wantState, again[0].Status.State, "returned status must not alias the cache")
		})
	}
}

func TestSuspendedRunningBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		states []string
		parked int
	}{
		{name: "running", states: []string{vmPowerStateRunning, vmPowerStateRunning, vmPowerStateRunning}},
		{name: "partly parked", states: []string{vmPowerStateRunning, vmPowerStateDeallocated, vmPowerStateDeallocated}, parked: 2},
		{name: "fully parked", states: []string{vmPowerStateDeallocated, vmPowerStateDeallocated, vmPowerStateDeallocated}, parked: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider, group := newSuspendedProvider(t, &suspendedWorld{states: tc.states}, fake.NewClientset(), 2, 3)
			total, err := group.TargetSize(t.Context())
			require.NoError(t, err)
			require.Equal(t, 3, total)
			require.Equal(t, 2+tc.parked, group.MinSize(t.Context()))
			require.Equal(t, 3+tc.parked, group.MaxSize(t.Context()))

			running := total - tc.parked
			require.Equal(t, 3-running, group.MaxSize(t.Context())-total, "selection and estimator headroom")
			require.Equal(t, 2-running, group.MinSize(t.Context())-total, "minimum-size growth")
			require.Equal(t, fmt.Sprintf("pool (%d:%d)", 2+tc.parked, 3+tc.parked), group.Debug(t.Context()))

			cache, client := provider.azureManager.azureCache, provider.azureManager.azClient
			provider.azureManager.azureCache, provider.azureManager.azClient = nil, nil
			func() {
				defer func() { provider.azureManager.azureCache, provider.azureManager.azClient = cache, client }()
				require.Equal(t, 2+tc.parked, group.MinSize(t.Context()), "bounds must not read Azure or its resource cache")
				require.Equal(t, 3+tc.parked, group.MaxSize(t.Context()))
			}()
		})
	}
}

func TestSuspendedGetOptions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		startup       time.Duration
		provision     time.Duration
		tags          map[string]string
		missingVMSS   bool
		deleteMode    bool
		wantStartup   time.Duration
		wantProvision time.Duration
	}{
		{name: "defaults", startup: 15 * time.Minute, provision: 15 * time.Minute,
			wantStartup: 21*time.Minute + time.Second, wantProvision: 15 * time.Minute},
		{name: "global provision", startup: time.Minute, provision: 30 * time.Minute,
			wantStartup: 36*time.Minute + time.Second, wantProvision: 30 * time.Minute},
		{name: "longer global startup", startup: time.Hour, provision: 15 * time.Minute,
			wantStartup: time.Hour, wantProvision: 15 * time.Minute},
		{name: "pool provision", startup: 15 * time.Minute, provision: 15 * time.Minute,
			tags:        map[string]string{config.DefaultMaxNodeProvisionTimeKey: "30m"},
			wantStartup: 36*time.Minute + time.Second, wantProvision: 30 * time.Minute},

		{name: "longer pool startup", startup: 15 * time.Minute, provision: 15 * time.Minute,
			tags:        map[string]string{config.DefaultMaxNodeStartupTimeKey: "1h"},
			wantStartup: time.Hour, wantProvision: 15 * time.Minute},
		{name: "shorter pool startup", startup: time.Hour, provision: 15 * time.Minute,
			tags:        map[string]string{config.DefaultMaxNodeStartupTimeKey: "1m"},
			wantStartup: 21*time.Minute + time.Second, wantProvision: 15 * time.Minute},
		{name: "invalid pool options", startup: 15 * time.Minute, provision: 15 * time.Minute,
			tags:        map[string]string{config.DefaultMaxNodeStartupTimeKey: "invalid", config.DefaultMaxNodeProvisionTimeKey: "invalid"},
			wantStartup: 21*time.Minute + time.Second, wantProvision: 15 * time.Minute},

		{name: "missing VMSS", startup: 15 * time.Minute, provision: 15 * time.Minute, missingVMSS: true,
			wantStartup: 21*time.Minute + time.Second, wantProvision: 15 * time.Minute},
		{name: "Delete unchanged", startup: time.Minute, provision: time.Hour, deleteMode: true,
			tags:        map[string]string{config.DefaultMaxNodeStartupTimeKey: "2h", config.DefaultMaxNodeProvisionTimeKey: "2h"},
			wantStartup: time.Minute, wantProvision: time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, group := newSuspendedProvider(t, &suspendedWorld{states: []string{vmPowerStateRunning}},
				fake.NewClientset(), 0, 3)
			group.deallocate = !tc.deleteMode
			group.manager.azureCache.autoscalingOptions[azureRef{Name: group.Name}] = tc.tags

			if tc.missingVMSS {
				delete(group.manager.azureCache.scaleSets, group.Name)
			}

			defaults := config.NodeGroupAutoscalingOptions{
				MaxNodeStartupTime: tc.startup, MaxNodeProvisionTime: tc.provision,
				ScaleDownUnneededTime: time.Hour, ScaleDownUnreadyTime: time.Minute,
				ScaleDownUtilizationThreshold: 0.3, IgnoreDaemonSetsUtilization: true,
			}
			options, err := group.GetOptions(t.Context(), defaults)
			require.NoError(t, err)

			expected := defaults
			expected.MaxNodeStartupTime, expected.MaxNodeProvisionTime = tc.wantStartup, tc.wantProvision
			require.Equal(t, expected, *options)

			processor := nodegroupconfig.NewDefaultNodeGroupConfigProcessor(defaults)
			startup, err := processor.GetMaxNodeStartupTime(t.Context(), group)
			require.NoError(t, err)

			provision, err := processor.GetMaxNodeProvisionTime(t.Context(), group)
			require.NoError(t, err)
			require.Equal(t, tc.wantStartup, startup)
			require.Equal(t, tc.wantProvision, provision)
			if tc.deleteMode {
				return
			}

			transition := metav1.NewTime(time.Unix(100, 999999999))
			data, err := json.Marshal(transition)
			require.NoError(t, err)

			var stored metav1.Time

			require.NoError(t, json.Unmarshal(data, &stored))

			requestTime := transition.Add(2 * vmssContextTimeout)
			deadline := requestTime.Add(provision)

			require.True(t, stored.Add(startup).After(deadline),
				"the resumed Node must remain NotStarted past the scale-up deadline")
		})
	}
}

func TestSuspendedBoundsKeepLastSnapshotOnError(t *testing.T) {
	world := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateDeallocated}}
	_, group := newSuspendedProvider(t, world, fake.NewClientset(), 1, 3)

	require.Equal(t, 2, group.MinSize(t.Context()))
	require.Equal(t, 4, group.MaxSize(t.Context()))
	world.mu.Lock()
	world.inventoryErr = errors.New("inventory unavailable")
	world.mu.Unlock()
	group.invalidateSuspendedInventory()
	_, err := group.Nodes(t.Context())
	require.ErrorContains(t, err, "inventory unavailable")

	_, err = group.TargetSize(t.Context())
	require.ErrorContains(t, err, "inventory unavailable")
	require.ErrorContains(t, group.IncreaseSize(t.Context(), 1), "inventory unavailable")
	require.Equal(t, 2, group.MinSize(t.Context()))
	require.Equal(t, 4, group.MaxSize(t.Context()))
	require.Empty(t, world.history())
}

func TestSuspendedInventoryDoesNotReuseVMIDOverride(t *testing.T) {
	world := &suspendedWorld{states: []string{vmPowerStateDeallocated}, identities: map[int]string{}}
	_, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
	group.recordPower("vm-0", suspendedPowerOperation{})
	instances, err := group.Nodes(t.Context())
	require.NoError(t, err)
	require.Equal(t, cloudprovider.InstanceCreating, instances[0].Status.State)
	world.mu.Lock()
	world.identities[0] = "replacement"
	world.mu.Unlock()
	group.invalidateSuspendedInventory()
	instances, err = group.Nodes(t.Context())
	require.NoError(t, err)
	require.Equal(t, cloudprovider.InstanceRunning, instances[0].Status.State)

	_, pending := group.powerOperation("vm-0")
	require.False(t, pending)
}

func TestSuspendedMutationsRequireFreshInventory(t *testing.T) {
	node := suspendedTestNode(0, false)
	world := &suspendedWorld{states: []string{vmPowerStateRunning}}
	_, group := newSuspendedProvider(t, world, fake.NewClientset(node), 0, 3)
	inventoryErr := errors.New("inventory unavailable")
	world.mu.Lock()
	world.inventoryErr = inventoryErr
	world.mu.Unlock()

	require.ErrorIs(t, group.IncreaseSize(t.Context(), 1), inventoryErr)
	require.ErrorIs(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}), inventoryErr)
	require.Empty(t, world.history())
}

func TestSuspendedRediscoveryKeepsPendingOperations(t *testing.T) {
	world := &suspendedWorld{states: []string{vmPowerStateDeallocated}}
	provider, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
	group.recordPower("vm-0", suspendedPowerOperation{})
	next, err := NewScaleSet(&dynamic.NodeGroupSpec{Name: "pool", MinSize: 0, MaxSize: 4},
		provider.azureManager, 1, false)
	require.NoError(t, err)
	require.True(t, provider.azureManager.RegisterNodeGroup(next))

	_, pending := next.powerOperation("vm-0")
	require.True(t, pending)
	require.NoError(t, next.IncreaseSize(t.Context(), 1))
	require.Equal(t, []string{"grow:2"}, world.history())

	_, pending = next.powerOperation("vm-0")
	require.True(t, pending, "new demand preserves the original accepted VMID")
}
