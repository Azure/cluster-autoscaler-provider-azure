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
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v8"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachinescalesetclient/mock_virtualmachinescalesetclient"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
)

func TestParseAzureNodeGroupSpec(t *testing.T) {
	for _, tc := range []struct {
		name, value, policy string
		zero                bool
		min, max            int
		labels              map[string]string
		taints              string
		wantErr             string
	}{
		{name: "core", value: "0:10:pool", zero: true, max: 10, policy: scaleDownPolicyDelete},
		{name: "standard", value: "1:5:pool", min: 1, max: 5, policy: scaleDownPolicyDelete},
		{name: "Delete", value: "0:5:Delete:pool", zero: true, max: 5, policy: scaleDownPolicyDelete},
		{name: "Deallocate", value: "0:5:Deallocate:pool", zero: true, max: 5, policy: scaleDownPolicyDeallocate},

		{
			name: "empty labels", value: "0:5:Delete:pool:{}|", zero: true, max: 5,
			policy: scaleDownPolicyDelete, labels: map[string]string{},
		},

		{
			name: "labels only", value: `0:5:Delete:pool:{"env":"prod"}`, zero: true, max: 5,
			policy: scaleDownPolicyDelete, labels: map[string]string{"env": "prod"},
		},

		{
			name:  "labels and taints with colons",
			value: `0:5:Deallocate:pool:{"zone":"east:1"}|key=value:NoSchedule,other=yes:NoExecute`,
			zero:  true, max: 5, policy: scaleDownPolicyDeallocate,
			labels: map[string]string{"zone": "east:1"}, taints: "key=value:NoSchedule,other=yes:NoExecute",
		},

		{name: "too few fields", value: "1:5", wantErr: "wrong nodes configuration"},
		{name: "invalid minimum", value: "wrong:5:pool", wantErr: "failed to set min size"},
		{name: "invalid maximum", value: "1:wrong:Delete:pool", wantErr: "failed to set max size"},
		{name: "negative minimum", value: "-1:5:pool", zero: true, wantErr: "min size must be >= 0"},
		{name: "standard zero", value: "0:5:pool", wantErr: "min size must be >= 1"},
		{name: "maximum below minimum", value: "5:1:Delete:pool", wantErr: "max size must be greater"},
		{name: "empty name", value: "1:5:Delete:", wantErr: "name must not be blank"},
		{name: "unknown policy", value: "1:5:Keep:pool:{}|", wantErr: "want Delete or Deallocate"},
		{name: "deallocate invalid maximum", value: "1:wrong:Deallocate:pool", wantErr: "failed to set max size"},
		{name: "deallocate invalid name", value: "1:5:Deallocate:", wantErr: "name must not be blank"},
		{name: "invalid json", value: "1:5:Deallocate:pool:{|", wantErr: "invalid labels"},
		{name: "empty fifth field", value: "1:5:Delete:pool:", wantErr: "invalid labels"},
		{name: "numeric label", value: `1:5:Delete:pool:{"team":42}|`, wantErr: "invalid labels"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := parseAzureNodeGroupSpec(tc.value, tc.zero)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Nil(t, spec.NodeGroupSpec)
				return
			}

			require.NoError(t, err)
			require.Equal(t, "pool", spec.Name)
			require.Equal(t, tc.min, spec.MinSize)
			require.Equal(t, tc.max, spec.MaxSize)
			require.Equal(t, tc.zero, spec.SupportScaleToZero)
			require.Equal(t, tc.policy, spec.policy)
			require.Equal(t, tc.labels, spec.labels)
			require.Equal(t, tc.taints, spec.taints)
		})
	}
}

func TestHasExplicitDeallocatePolicy(t *testing.T) {
	for _, tc := range []struct {
		name  string
		specs []string
		want  bool
	}{
		{name: "none"},
		{name: "Delete", specs: []string{"0:5:Delete:pool:{}|"}},
		{name: "four fields", specs: []string{"0:5:Deallocate:pool"}, want: true},
		{name: "later spec", specs: []string{"0:5:pool", "0:5:Deallocate:other"}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, hasExplicitDeallocatePolicy(tc.specs))
		})
	}
}

func TestSuspendedGroupGates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*AzureManager, *armcompute.VirtualMachineScaleSet)
	}{
		{name: "standard", mutate: func(m *AzureManager, _ *armcompute.VirtualMachineScaleSet) {
			m.config.VMType = "standard"
		}},
		{name: "flex", mutate: func(_ *AzureManager, set *armcompute.VirtualMachineScaleSet) {
			set.Properties.OrchestrationMode = ptr.To(armcompute.OrchestrationModeFlexible)
		}},

		{name: "Low", mutate: func(_ *AzureManager, set *armcompute.VirtualMachineScaleSet) {
			set.Properties.VirtualMachineProfile.Priority = ptr.To(armcompute.VirtualMachinePriorityTypesLow)
		}},
		{name: "AKS managed", mutate: func(_ *AzureManager, set *armcompute.VirtualMachineScaleSet) {
			set.Tags["AKS-MANAGED-POOLNAME"] = ptr.To("workers")
		}},

		{name: "ephemeral", mutate: func(_ *AzureManager, set *armcompute.VirtualMachineScaleSet) {
			set.Properties.VirtualMachineProfile.StorageProfile.OSDisk.DiffDiskSettings = &armcompute.DiffDiskSettings{}
		}},

		{name: "unmanaged disk", mutate: func(_ *AzureManager, set *armcompute.VirtualMachineScaleSet) {
			set.Properties.VirtualMachineProfile.StorageProfile.OSDisk.ManagedDisk = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := &suspendedWorld{states: []string{vmPowerStateRunning}}
			provider, original := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			m := provider.azureManager
			m.UnregisterNodeGroup(original)
			m.config.Deallocate = false
			set := world.vmss()
			tc.mutate(m, set)
			m.azureCache.setScaleSet("pool", set)
			_, err := m.buildNodeGroupFromSpec("1:3:Deallocate:pool")
			var unsupported *unsupportedDeallocateError
			require.ErrorAs(t, err, &unsupported)

		})
	}
}

func TestExtendedScaleSetNodeTemplate(t *testing.T) {
	manager := newTestAzureManager(t)
	vmss := *manager.azureCache.getScaleSets()["test-asg"]
	vmss.Tags = map[string]*string{
		nodeLabelTagName + "env":      ptr.To("tag"),
		nodeLabelTagName + "tag-only": ptr.To("tag"),
		nodeTaintTagName + "from-tag": ptr.To("tag:NoExecute"),
		legacyPoolNameTag:             ptr.To("pool"),
	}
	manager.azureCache.setScaleSet("test-asg", &vmss)

	group, err := manager.buildNodeGroupFromSpec(`0:5:Delete:test-asg:{"env":"prod"}|from-spec=prod:NoSchedule`)
	require.NoError(t, err)

	scaleSet, ok := group.(*ScaleSet)
	require.True(t, ok)

	scaleSet.enableLabelPredictionsOnTemplate = true
	nodeInfo, err := scaleSet.TemplateNodeInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, "prod", nodeInfo.Node().Labels["env"])
	require.Equal(t, map[string]string{"env": "prod"}, scaleSet.labels)
	require.Empty(t, nodeInfo.Node().Labels["tag-only"])
	require.Equal(t, []apiv1.Taint{
		{Key: "from-spec", Value: "prod", Effect: apiv1.TaintEffectNoSchedule},
	}, nodeInfo.Node().Spec.Taints)

	legacy, err := manager.buildNodeGroupFromSpec("0:5:test-asg")
	require.NoError(t, err)

	legacyInfo, err := legacy.TemplateNodeInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, "tag", legacyInfo.Node().Labels["env"])
	require.Equal(t, "tag", legacyInfo.Node().Labels["tag-only"])
	require.Equal(t, []apiv1.Taint{
		{Key: "from-tag", Value: "tag", Effect: apiv1.TaintEffectNoExecute},
	}, legacyInfo.Node().Spec.Taints)
}

func TestExtendedVMPoolNodeTemplate(t *testing.T) {
	manager := newTestAzureManager(t)
	manager.azClient.agentPoolClient = NewMockAgentPoolsClient(gomock.NewController(t))
	pool := getTestVMsAgentPool(false)
	pool.Properties.NodeLabels = map[string]*string{"env": ptr.To("from-pool"), "pool-only": ptr.To("pool")}
	pool.Properties.NodeTaints = []*string{ptr.To("from-pool=yes:NoExecute")}
	manager.azureCache.vmsPoolMap[vmsAgentPoolName] = pool

	group, err := manager.buildNodeGroupFromSpec(fmt.Sprintf(
		`0:5:Delete:%s:{"env":"prod"}|from-spec=yes:NoSchedule`, vmsNodeGroupName,
	))
	require.NoError(t, err)

	nodeInfo, err := group.TemplateNodeInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, "prod", nodeInfo.Node().Labels["env"])
	require.Equal(t, "pool", nodeInfo.Node().Labels["pool-only"])
	require.ElementsMatch(t, []apiv1.Taint{
		{Key: "from-pool", Value: "yes", Effect: apiv1.TaintEffectNoExecute},
		{Key: "from-spec", Value: "yes", Effect: apiv1.TaintEffectNoSchedule},
	}, nodeInfo.Node().Spec.Taints)

	other, err := manager.buildNodeGroupFromSpec(fmt.Sprintf(
		`0:5:Delete:%s/Standard_D4_v2:{"other":"second"}|`, vmsAgentPoolName,
	))
	require.NoError(t, err)

	otherInfo, err := other.TemplateNodeInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, "from-pool", otherInfo.Node().Labels["env"])
	require.Equal(t, "second", otherInfo.Node().Labels["other"])
	require.Equal(t, []apiv1.Taint{
		{Key: "from-pool", Value: "yes", Effect: apiv1.TaintEffectNoExecute},
	}, otherInfo.Node().Spec.Taints)
	require.Equal(t, "from-pool", *manager.azureCache.getVMsPoolMap()[vmsAgentPoolName].Properties.NodeLabels["env"])
	require.NotContains(t, manager.azureCache.getVMsPoolMap()[vmsAgentPoolName].Properties.NodeLabels, "other")
}

func TestSuspendedGlobalPolicy(t *testing.T) {
	world := &suspendedWorld{states: []string{vmPowerStateRunning}}
	provider, _ := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
	m := provider.azureManager
	group, err := m.buildNodeGroupFromSpec("0:3:Delete:pool")
	require.NoError(t, err)

	scaleSet := group.(*ScaleSet)
	require.True(t, scaleSet.suspendsParkedNodes(), "global policy also applies to explicit Delete")

	set := world.vmss()
	set.Properties.VirtualMachineProfile.StorageProfile.OSDisk.DiffDiskSettings = &armcompute.DiffDiskSettings{}
	set.Tags["discover"], set.Tags["min"], set.Tags["max"] = ptr.To("yes"), ptr.To("0"), ptr.To("3")
	m.azureCache.setScaleSet("pool", set)
	_, err = m.buildNodeGroupFromSpec("0:3:Delete:pool")
	require.Error(t, err, "unsupported global groups must not fall back to Delete")
	specs, err := ParseLabelAutoDiscoverySpecs(cloudprovider.NodeGroupDiscoveryOptions{
		NodeGroupAutoDiscoverySpecs: []string{"label:discover=yes"},
	})
	require.NoError(t, err)

	groups, err := m.getFilteredScaleSets(specs)
	require.NoError(t, err)
	require.Empty(t, groups)
}

func TestSuspendedSelectedGroupAndVMsPoolGate(t *testing.T) {
	world := &suspendedWorld{states: []string{vmPowerStateRunning}}
	provider, original := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
	m := provider.azureManager
	m.UnregisterNodeGroup(original)
	m.config.Deallocate = false

	m.azureCache.vmsPoolMap["workers"] = armcontainerservice.AgentPool{}
	_, err := m.buildNodeGroupFromSpec("0:3:Deallocate:workers/Standard_D4_v2")
	require.ErrorContains(t, err, "VMs pools are not supported")
}

func TestSuspendedExtendedGroup(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(fmt.Sprintf("global=%t", global), func(t *testing.T) {
			world := &suspendedWorld{states: []string{vmPowerStateRunning}}
			provider, original := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			m := provider.azureManager
			m.UnregisterNodeGroup(original)
			m.config.Deallocate = global

			require.NoError(t, m.fetchExplicitNodeGroups([]string{
				`0:5:Deallocate:pool:{"zone":"east:1"}|key=value:NoSchedule`,
			}))
			require.Len(t, m.getNodeGroups(), 1)

			group := m.getNodeGroups()[0].(*ScaleSet)

			require.True(t, group.suspendsParkedNodes())
			require.True(t, group.explicitDeallocate)
			require.Equal(t, map[string]string{"zone": "east:1"}, group.labels)
			require.Equal(t, "key=value:NoSchedule", group.taints)
		})
	}
}

func TestSuspendedExplicitLoadsVMSSCache(t *testing.T) {
	for _, tc := range []struct {
		name    string
		listErr error
	}{
		{name: "inventory loaded before gate"},
		{name: "failed inventory is not unsupported", listErr: errors.New("inventory unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := &suspendedWorld{states: []string{vmPowerStateRunning}}
			provider, _ := newSuspendedProvider(t, world, fake.NewClientset(), 0, 3)
			ctrl := gomock.NewController(t)
			client := mock_virtualmachinescalesetclient.NewMockInterface(ctrl)
			client.EXPECT().List(gomock.Any(), "rg").DoAndReturn(
				func(context.Context, string) ([]*armcompute.VirtualMachineScaleSet, error) {
					return []*armcompute.VirtualMachineScaleSet{world.vmss()}, tc.listErr
				}).AnyTimes()
			provider.azureManager.azClient.virtualMachineScaleSetsClient = client
			cfg := strings.Replace(validAzureCfg, `"resourceGroup": "fakeId"`, `"resourceGroup": "rg"`, 1)
			manager, err := createAzureManagerInternal(strings.NewReader(cfg),
				cloudprovider.NodeGroupDiscoveryOptions{NodeGroupSpecs: []string{"0:3:Deallocate:pool"}},
				provider.azureManager.azClient)
			if tc.listErr != nil {
				require.ErrorContains(t, err, "load VMSS cache for deallocate groups")
				require.ErrorIs(t, err, tc.listErr)
				return
			}

			require.NoError(t, err)
			t.Cleanup(manager.Cleanup)

			require.Len(t, manager.getNodeGroups(), 1)
			require.True(t, manager.getNodeGroups()[0].(*ScaleSet).suspendsParkedNodes())
		})
	}
}
