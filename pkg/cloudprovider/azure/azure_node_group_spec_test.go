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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
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
