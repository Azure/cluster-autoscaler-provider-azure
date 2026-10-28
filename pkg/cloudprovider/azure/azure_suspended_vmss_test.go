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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
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
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/policy/retryrepectthrottled"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachinescalesetclient/mock_virtualmachinescalesetclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachinescalesetvmclient/mock_virtualmachinescalesetvmclient"
	providerconfig "sigs.k8s.io/cloud-provider-azure/pkg/provider/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/config/dynamic"
	catest "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const suspendedVMSSID = "/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/virtualMachineScaleSets/pool"

type suspendedWorld struct {
	// name defaults to "pool".
	name           string
	mu             sync.Mutex
	states         []string
	priority       armcompute.VirtualMachinePriorityTypes
	eviction       armcompute.VirtualMachineEvictionPolicyTypes
	identities     map[int]string
	provisioning   map[int]string
	deleted        map[int]bool
	events         []string
	startError     map[int]error
	parkError      map[int]error
	nilPark        bool
	nilStart       bool
	startResult    func(int) error
	parkResult     func(int) error
	inventoryErr   error
	inventoryReads int
	growthErr      error
}

func (w *suspendedWorld) history() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.events)
}

func (w *suspendedWorld) setPowerState(i int, state string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.states[i] = state
}

func (w *suspendedWorld) poolName() string {
	if w.name == "" {
		return "pool"
	}
	return w.name
}

func (w *suspendedWorld) label() string {
	if w.name == "" {
		return "workers"
	}
	return w.name
}

func (w *suspendedWorld) vmssID() string {
	return strings.TrimSuffix(suspendedVMSSID, "pool") + w.poolName()
}

func (w *suspendedWorld) vmss() *armcompute.VirtualMachineScaleSet {
	w.mu.Lock()
	defer w.mu.Unlock()
	set := newTestVMSSList(int64(len(w.states)-len(w.deleted)), w.poolName(), "eastus", armcompute.OrchestrationModeUniform)[0]
	set.ID = ptr.To(w.vmssID())
	set.Tags = map[string]*string{nodeLabelTagName + "pool": ptr.To(w.label())}
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
	w.inventoryReads++
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
			ID:         ptr.To(fmt.Sprintf("%s/virtualMachines/%d", w.vmssID(), i)),
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

func TestSuspendedRefreshIsolatesInventoryFailure(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			world := &suspendedWorld{states: []string{vmPowerStateRunning}}
			provider, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			world.inventoryErr = retryrepectthrottled.ErrTooManyRequest
			group.invalidateSuspendedInventory()
			if missing {
				provider.azureManager.azureCache.mutex.Lock()
				provider.azureManager.azureCache.scaleSets = map[string]*armcompute.VirtualMachineScaleSet{}
				provider.azureManager.azureCache.mutex.Unlock()
			}
			require.NoError(t, provider.azureManager.reconcileSuspendedNodes())
			instances, err := group.Nodes(t.Context())
			require.NoError(t, err)
			if missing {
				require.Empty(t, instances)
			} else {
				require.Len(t, instances, 1)
			}
			require.NoError(t, provider.azureManager.azureCache.regenerate())
		})
	}
}

func TestSuspendedConstructionDoesNotReadInventory(t *testing.T) {
	world := &suspendedWorld{states: []string{vmPowerStateDeallocated}}
	manager := newSuspendedManager(t, fake.NewClientset(), world)
	world.inventoryErr = errors.New("inventory unavailable")
	group, err := NewScaleSet(&dynamic.NodeGroupSpec{Name: "pool", MaxSize: 3}, manager, 1, false)
	require.NoError(t, err)
	require.Zero(t, world.inventoryReads)
	manager.RegisterNodeGroup(group)
	world.inventoryErr = nil
	require.NoError(t, manager.Refresh())
	require.Equal(t, 1, group.MinSize(t.Context()))
}

func TestSuspendedInventoryCapacityMismatch(t *testing.T) {
	world := &suspendedWorld{states: []string{vmPowerStateRunning}}
	_, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
	group.sizeMutex.Lock()
	group.curSize = 2
	group.sizeMutex.Unlock()
	reads := world.inventoryReads
	for range 3 {
		_, err := group.TargetSize(t.Context())
		require.NoError(t, err)
	}
	require.Equal(t, reads+1, world.inventoryReads)
}

func TestSuspendedInventoryRefreshesAfterCapacityPoll(t *testing.T) {
	for _, operation := range []string{"grow", "delete", "delete retry"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				world := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateRunning}}
				provider, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
				resizeClient := NewMockVMSSDeleteClient(gomock.NewController(t))
				group.manager.azClient.vmssClientForDelete = resizeClient
				finish := make(chan struct{})
				defer close(finish)
				grow := operation == "grow"
				complete := func() error {
					<-finish
					world.mu.Lock()
					defer world.mu.Unlock()
					if grow {
						world.states = append(world.states, vmPowerStateStarting)
					} else {
						world.deleted = map[int]bool{1: true}
					}
					return nil
				}
				wantSize := 3
				instance := suspendedTestNode(2, false)
				if grow {
					poller, err := suspendedTestPoller[armcompute.VirtualMachineScaleSetsClientCreateOrUpdateResponse](complete)
					require.NoError(t, err)
					resizeClient.EXPECT().BeginCreateOrUpdate(gomock.Any(), "rg", "pool", gomock.Any(), gomock.Any()).Return(poller, nil)
					require.NoError(t, group.IncreaseSize(t.Context(), 1))
				} else {
					wantSize = 1
					instance = suspendedTestNode(1, false)
					poller, err := suspendedTestPoller[armcompute.VirtualMachineScaleSetsClientDeleteInstancesResponse](complete)
					require.NoError(t, err)
					initial := poller
					if operation == "delete retry" {
						initial = newTestPollerWithError(&azcore.ResponseError{ErrorCode: "OperationPreempted"})
					}
					first := resizeClient.EXPECT().BeginDeleteInstances(gomock.Any(), "rg", "pool", gomock.Any(), gomock.Any()).Return(initial, nil)
					if operation == "delete retry" {
						resizeClient.EXPECT().BeginDeleteInstances(gomock.Any(), "rg", "pool", gomock.Any(), gomock.Any()).Return(poller, nil).After(first)
					}
					require.NoError(t, group.DeleteInstances([]*azureRef{{Name: instance.Spec.ProviderID}}, false))
				}
				synctest.Wait()

				for range 2 {
					instances, err := group.Nodes(t.Context())
					require.NoError(t, err)
					require.Len(t, instances, 2, "Azure's instance list still predates the capacity change")
				}
				require.Equal(t, int64(wantSize), group.suspendedInventorySize)
				finish <- struct{}{}
				synctest.Wait()

				instances, err := group.Nodes(t.Context())
				require.NoError(t, err)
				require.Len(t, instances, wantSize, "poll completion must refresh the earlier mismatched inventory")
				require.NoError(t, provider.azureManager.azureCache.regenerate())
				found, err := provider.azureManager.azureCache.FindForInstance(&azureRef{Name: instance.Spec.ProviderID}, "vmss")
				require.NoError(t, err)
				if grow {
					require.Same(t, group, found)
				} else {
					require.Nil(t, found)
				}
			})
		})
	}
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

// suspendedWorlds serves several scale sets by name.
type suspendedWorlds []*suspendedWorld

func (ws suspendedWorlds) world(name string) (*suspendedWorld, error) {
	for _, w := range ws {
		if strings.EqualFold(w.poolName(), name) {
			return w, nil
		}
	}
	return nil, fmt.Errorf("unexpected VMSS %s", name)
}

func (ws suspendedWorlds) BeginStart(
	_ context.Context, _, name, id string,
	_ *armcompute.VirtualMachineScaleSetVMsClientBeginStartOptions,
) (*azruntime.Poller[armcompute.VirtualMachineScaleSetVMsClientStartResponse], error) {
	w, err := ws.world(name)
	if err != nil {
		return nil, err
	}
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

		w.setPowerState(i, vmPowerStateRunning)
		return nil
	})
}

func (ws suspendedWorlds) BeginDeallocate(
	_ context.Context, _, name, id string,
	_ *armcompute.VirtualMachineScaleSetVMsClientBeginDeallocateOptions,
) (*azruntime.Poller[armcompute.VirtualMachineScaleSetVMsClientDeallocateResponse], error) {
	w, err := ws.world(name)
	if err != nil {
		return nil, err
	}
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

		w.setPowerState(i, vmPowerStateDeallocated)
		return nil
	})
}

func newSuspendedProvider(
	t *testing.T, world *suspendedWorld, client *fake.Clientset, minimum, maximum int,
) (*AzureCloudProvider, *ScaleSet) {
	t.Helper()
	manager := newSuspendedManager(t, client, world)
	group, err := NewScaleSet(&dynamic.NodeGroupSpec{Name: "pool", MinSize: minimum, MaxSize: maximum},
		manager, *world.vmss().SKU.Capacity, false)
	require.NoError(t, err)
	manager.RegisterNodeGroup(group)

	require.NoError(t, manager.forceRefresh())

	provider, err := BuildAzureCloudProvider(manager, cloudprovider.NewResourceLimiter(map[string]int64{}, map[string]int64{}))
	require.NoError(t, err)
	return provider.(*AzureCloudProvider), group
}

// newSuspendedManager returns a manager backed by fake scale sets, with no node groups registered.
func newSuspendedManager(t *testing.T, client *fake.Clientset, worlds ...*suspendedWorld) *AzureManager {
	t.Helper()
	ctrl := gomock.NewController(t)
	vmssClient := mock_virtualmachinescalesetclient.NewMockInterface(ctrl)
	vmssClient.EXPECT().List(gomock.Any(), "rg").DoAndReturn(
		func(context.Context, string) ([]*armcompute.VirtualMachineScaleSet, error) {
			sets := make([]*armcompute.VirtualMachineScaleSet, 0, len(worlds))
			for _, w := range worlds {
				sets = append(sets, w.vmss())
			}
			return sets, nil
		}).AnyTimes()
	vmssClient.EXPECT().Get(gomock.Any(), "rg", gomock.Any(), nil).DoAndReturn(
		func(_ context.Context, _ string, name string,
			_ *armcompute.ExpandTypesForGetVMScaleSets,
		) (*armcompute.VirtualMachineScaleSet, error) {
			w, err := suspendedWorlds(worlds).world(name)
			if err != nil {
				return nil, err
			}
			return w.vmss(), nil
		}).AnyTimes()

	vmClient := mock_virtualmachineclient.NewMockInterface(ctrl)
	vmClient.EXPECT().List(gomock.Any(), "rg").Return([]*armcompute.VirtualMachine{}, nil).AnyTimes()

	instanceClient := mock_virtualmachinescalesetvmclient.NewMockInterface(ctrl)
	instanceClient.EXPECT().ListVMInstanceView(gomock.Any(), "rg", gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, name string) ([]*armcompute.VirtualMachineScaleSetVM, error) {
			w, err := suspendedWorlds(worlds).world(name)
			if err != nil {
				return nil, err
			}
			return w.vms()
		}).AnyTimes()

	resizeClient := NewMockVMSSDeleteClient(ctrl)
	resizeClient.EXPECT().BeginCreateOrUpdate(gomock.Any(), "rg", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, name string, vmss armcompute.VirtualMachineScaleSet,
			_ *armcompute.VirtualMachineScaleSetsClientBeginCreateOrUpdateOptions,
		) (*azruntime.Poller[armcompute.VirtualMachineScaleSetsClientCreateOrUpdateResponse], error) {
			world, err := suspendedWorlds(worlds).world(name)
			if err != nil {
				return nil, err
			}
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

	resizeClient.EXPECT().BeginDeleteInstances(gomock.Any(), "rg", gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, name string, ids armcompute.VirtualMachineScaleSetVMInstanceRequiredIDs,
			_ *armcompute.VirtualMachineScaleSetsClientBeginDeleteInstancesOptions,
		) (*azruntime.Poller[armcompute.VirtualMachineScaleSetsClientDeleteInstancesResponse], error) {
			world, err := suspendedWorlds(worlds).world(name)
			if err != nil {
				return nil, err
			}
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

	explicit := make(map[string]bool, len(worlds))
	for _, w := range worlds {
		explicit[w.poolName()] = true
	}
	manager := &AzureManager{
		config: &Config{
			Config:     providerconfig.Config{VMType: "vmss", ResourceGroup: "rg", Location: "eastus"},
			Deallocate: true,
		},
		env: azure.PublicCloud, kubeClient: client, cordonNodeBeforeTerminate: true,
		explicitlyConfigured: explicit,
		azClient: &azClient{
			virtualMachineScaleSetsClient: vmssClient, virtualMachineScaleSetVMsClient: instanceClient,
			virtualMachinesClient: vmClient, vmssClientForDelete: resizeClient, vmssPowerClient: suspendedWorlds(worlds),
		},
	}

	var err error
	manager.azureCache, err = newAzureCache(manager.azClient, time.Minute, *manager.config)
	require.NoError(t, err)
	t.Cleanup(manager.Cleanup)

	require.NoError(t, manager.forceRefresh())
	return manager
}

func getTestNode(t *testing.T, client *fake.Clientset, name string) *apiv1.Node {
	t.Helper()
	node, err := client.CoreV1().Nodes().Get(t.Context(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return node
}

func suspendedTestNode(id int, suspended bool) *apiv1.Node {
	return suspendedPoolNode(&suspendedWorld{}, id, suspended)
}

func suspendedPoolNode(world *suspendedWorld, id int, suspended bool) *apiv1.Node {
	name := fmt.Sprintf("node-%d", id)
	if world.name != "" {
		name = fmt.Sprintf("%s-%d", world.name, id)
	}
	node := catest.BuildTestNode(name, 4000, 8*1024*1024*1024, catest.IsReady(true))
	node.UID = types.UID(fmt.Sprintf("old-%s", name))
	node.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	node.Spec.ProviderID = fmt.Sprintf("%s%s/virtualMachines/%d", azurePrefix, world.vmssID(), id)
	node.Spec.PodCIDR = fmt.Sprintf("10.244.%d.0/24", id)
	node.Labels = map[string]string{
		"pool": world.label(), apiv1.LabelHostname: node.Name,
		apiv1.LabelOSStable: "linux", apiv1.LabelArchStable: "amd64",
		apiv1.LabelInstanceTypeStable: "Standard_D4_v2", apiv1.LabelTopologyRegion: "eastus",
	}

	if suspended {
		setSuspendedCondition(node, apiv1.ConditionTrue, parkedReason, metav1.NewTime(time.Now().Add(-time.Hour)))
		node.Annotations = map[string]string{
			scaleDownDisabledAnnotation: "true", scaleDownDisabledOwnerAnnotation: "true",
		}
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
		missing    string
		hosted     string
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

		{name: "missing pool name", missing: "pool", wantError: "AKS pool name"},
		{name: "missing cluster name", missing: "cluster", wantError: "clusterName"},
		{name: "missing cluster resource group", missing: "resource group", wantError: "clusterResourceGroup"},
		{name: "missing client", missing: "client", wantError: "agent pool client"},
		{name: "hosted subscription without policy", hosted: "subscription", wantError: "explicit Deallocate policy"},
		{name: "hosted resource group without policy", hosted: "resource group", wantError: "explicit Deallocate policy"},
		{name: "hosted proxy without policy", hosted: "proxy", wantError: "explicit Deallocate policy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := &suspendedWorld{states: []string{vmPowerStateRunning}}
			_, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			client := NewMockAgentPoolsClient(gomock.NewController(t))
			group.manager.azClient.agentPoolClient = client
			cfg := group.manager.config
			cfg.ClusterName, cfg.ClusterResourceGroup = "cluster", "cluster-rg"
			vmss, err := group.getVMSSFromCache()
			require.NoError(t, err)

			vmss.Tags["aks-managed-cluster-name"] = ptr.To("cluster")
			vmss.Tags[agentpoolNameTag] = ptr.To("workers")

			switch tc.missing {
			case "pool":
				delete(vmss.Tags, agentpoolNameTag)
			case "cluster":
				cfg.ClusterName = ""
			case "resource group":
				cfg.ClusterResourceGroup = ""
			case "client":
				group.manager.azClient.agentPoolClient = nil
			}
			switch tc.hosted {
			case "subscription":
				cfg.HostedSubscriptionID = "hosted"
			case "resource group":
				cfg.HostedResourceGroup = "hosted"
			case "proxy":
				cfg.HostedResourceProxyURL = "https://hosted"
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
			}).Times(2)
		manager.azClient.agentPoolClient = client
		_, err = group.suspendedInventory(t.Context(), false)
		require.NoError(t, err)
		assert.Equal(t, 1, calls)

		mode = armcontainerservice.ScaleDownModeDelete

		time.Sleep(manager.azureCache.refreshInterval - time.Nanosecond)
		_, err = group.suspendedInventory(t.Context(), false)
		require.NoError(t, err)
		assert.Equal(t, 1, calls)
		time.Sleep(time.Nanosecond)
		_, err = group.suspendedInventory(t.Context(), false)
		assert.ErrorContains(t, err, "Deallocate scale-down mode")
		assert.Equal(t, 2, calls)
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
		{name: "Delete", spec: "0:3:Delete:pool"},
		{name: "global Delete", spec: "0:3:Delete:pool", global: true, wantError: true},
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
			group, err := m.buildNodeGroupFromSpec(tc.spec)

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
		{name: "updating deallocated", power: vmPowerStateDeallocated, provisioning: "Updating", wantState: cloudprovider.InstanceCreating},
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

func TestSuspendedBoundsKeepLastSnapshotOnError(t *testing.T) {
	node := suspendedTestNode(0, false)
	world := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateDeallocated}}
	_, group := newSuspendedProvider(t, world, fake.NewClientset(node), 1, 3)

	require.Equal(t, 2, group.MinSize(t.Context()))
	require.Equal(t, 4, group.MaxSize(t.Context()))
	world.mu.Lock()
	world.inventoryErr = errors.New("inventory unavailable")
	world.mu.Unlock()
	require.ErrorIs(t, group.IncreaseSize(t.Context(), 1), world.inventoryErr)
	require.ErrorIs(t, group.DeleteNodes(t.Context(), []*apiv1.Node{node}), world.inventoryErr)
	group.invalidateSuspendedInventory()
	_, err := group.Nodes(t.Context())
	require.ErrorContains(t, err, "inventory unavailable")

	_, err = group.TargetSize(t.Context())
	require.ErrorContains(t, err, "inventory unavailable")
	require.Equal(t, 2, group.MinSize(t.Context()))
	require.Equal(t, 4, group.MaxSize(t.Context()))
	require.Empty(t, world.history())
}

func TestSuspendedInventoryDoesNotReuseVMIDOverride(t *testing.T) {
	world := &suspendedWorld{states: []string{vmPowerStateDeallocated}, identities: map[int]string{}}
	_, group := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
	group.recordPower("vm-0", suspendedPowerOperation{park: true, polling: true})
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

func TestSuspendedRediscoveryKeepsPendingOperations(t *testing.T) {
	node := suspendedTestNode(0, true)
	world := &suspendedWorld{states: []string{vmPowerStateRunning}}
	provider, group := newSuspendedProvider(t, world, fake.NewClientset(node), 0, 3)
	group.recordPower("vm-0", suspendedPowerOperation{node: node, park: true, polling: true})
	next, err := NewScaleSet(&dynamic.NodeGroupSpec{Name: "pool", MinSize: 0, MaxSize: 4},
		provider.azureManager, 1, false)
	require.NoError(t, err)
	require.True(t, provider.azureManager.RegisterNodeGroup(next))

	_, pending := next.powerOperation("vm-0")
	require.True(t, pending)
	require.ErrorContains(t, next.IncreaseSize(t.Context(), 4), "parking is still in progress")
	require.Empty(t, world.history())

	_, pending = next.powerOperation("vm-0")
	require.True(t, pending, "new demand preserves the original accepted VMID")
}
