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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
)

func TestDecodeAKSSettings(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    string
		want    []string
		wantErr string
	}{
		{
			name: "default policy", data: `{"nodeGroups":[{"name":"pool","minSize":0,"maxSize":10}]}`,
			want: []string{"0:10:Delete:pool:{}|"},
		},

		{
			name: "labels and taints",
			data: `{"nodeGroups":[{"name":"pool/Standard_D2s_v3","minSize":1,"maxSize":50,"labels":{"team":"prod","rack":"east"},"taints":"special=true:NoSchedule"}]}`,
			want: []string{`1:50:Delete:pool/Standard_D2s_v3:{"rack":"east","team":"prod"}|special=true:NoSchedule`},
		},

		{
			name: "yaml", data: "nodeGroups:\n- name: pool\n  minSize: 2\n  maxSize: 5\n  labels:\n    team: prod\n",
			want: []string{`2:5:Delete:pool:{"team":"prod"}|`},
		},

		{
			name: "unrelated profile", data: `{"autoScalerProfile":{"scan-interval":"15s"},"nodeGroups":[]}`,
			want: []string{},
		},

		{
			name: "mixed policies",
			data: `{"nodeGroups":[{"name":"paused","minSize":1,"maxSize":5,"scaleDownPolicy":"Deallocate"},{"name":"active","minSize":0,"maxSize":10,"scaleDownPolicy":"Delete"}]}`,
			want: []string{"1:5:Deallocate:paused:{}|", "0:10:Delete:active:{}|"},
		},
		{
			name: "numeric label conversion", data: `{"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5,"labels":{"team":42}}]}`,
			want: []string{`1:5:Delete:pool:{"team":"42"}|`},
		},

		{
			name: "provider validates negative minimum",
			data: `{"nodeGroups":[{"name":"pool","minSize":-1,"maxSize":5}]}`,
			want: []string{"-1:5:Delete:pool:{}|"},
		},

		{name: "invalid groups", data: `{"nodeGroups":"pool"}`, wantErr: "cannot unmarshal"},
		{name: "blank name", data: `{"nodeGroups":[{"name":""}]}`, wantErr: "name must not be blank"},

		{
			name: "maximum below minimum", data: `{"nodeGroups":[{"name":"pool","minSize":10,"maxSize":5}]}`,
			wantErr: "max size must be at least min size",
		},

		{
			name: "unknown policy", data: `{"nodeGroups":[{"name":"pool","scaleDownPolicy":"Keep"}]}`,
			wantErr: `scaleDownPolicy "Keep" is not supported`,
		},

		{
			name: "invalid labels", data: `{"nodeGroups":[{"name":"pool","labels":["team"]}]}`,
			wantErr: "cannot unmarshal",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings, err := decodeAKSSettings([]byte(tc.data))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)

			specs, err := settings.nodeGroupSpecs()
			require.NoError(t, err)
			require.Equal(t, tc.want, specs)
		})
	}
}

func TestUseAKSSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	original := []string{"1:5:cli-pool"}
	discovery := []string{"label:cluster-autoscaler-enabled=true"}
	opts := config.AutoscalingOptions{NodeGroups: original, NodeGroupAutoDiscovery: discovery}
	fetcher, err := useAKSSettings(&opts, "")
	require.NoError(t, err)
	require.Nil(t, fetcher)
	require.Equal(t, original, opts.NodeGroups)
	require.Equal(t, discovery, opts.NodeGroupAutoDiscovery)

	fetcher, err = useAKSSettings(&opts, path)
	require.Error(t, err)
	require.NotNil(t, fetcher)
	require.Empty(t, opts.NodeGroups)
	require.Equal(t, discovery, opts.NodeGroupAutoDiscovery)

	writeAKSSettings(t, path, `{"nodeGroups":[{"name":"file-pool","minSize":2,"maxSize":8},{"name":"paused","minSize":0,"maxSize":5,"scaleDownPolicy":"Deallocate","labels":{"zone":"east:1"},"taints":"key=value:NoSchedule"}]}`)
	opts.NodeGroups = original
	fetcher, err = useAKSSettings(&opts, path)
	require.NoError(t, err)
	require.NotNil(t, fetcher)
	require.Equal(t, []string{"2:8:Delete:file-pool:{}|", `0:5:Deallocate:paused:{"zone":"east:1"}|key=value:NoSchedule`}, opts.NodeGroups)
	require.Equal(t, discovery, opts.NodeGroupAutoDiscovery)
}

func TestAKSSettingsFetcher(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	fetcher := newAKSSettingsFetcher(path)
	_, changed, err := fetcher.fetchIfChanged()
	require.Error(t, err)
	require.False(t, changed)

	for _, tc := range []struct {
		name    string
		data    string
		changed bool
		wantErr bool
	}{
		{name: "empty groups", data: `{"nodeGroups":[]}`},
		{name: "missing groups", data: `{}`},
		{name: "new group", data: `{"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5}]}`, changed: true},
		{name: "same group with explicit default", data: `{"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5,"scaleDownPolicy":"Delete"}]}`},
		{name: "profile only", data: `{"autoScalerProfile":{"scan-interval":"15s"},"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5}]}`},
		{name: "explicit empty labels", data: `{"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5,"labels":{}}]}`, changed: true},
		{name: "label change", data: `{"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5,"labels":{"env":"prod"}}]}`, changed: true},
		{name: "invalid update", data: `{"nodeGroups":"invalid"}`, wantErr: true},
		{name: "restored", data: `{"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5,"labels":{"env":"prod"}}]}`},
		{name: "taint change", data: `{"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5,"labels":{"env":"prod"},"taints":"key=value:NoSchedule"}]}`, changed: true},
		{name: "policy change", data: `{"nodeGroups":[{"name":"pool","minSize":1,"maxSize":5,"scaleDownPolicy":"Deallocate"}]}`, changed: true},
		{name: "remove groups", data: `{"nodeGroups":[]}`, changed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writeAKSSettings(t, path, tc.data)
			specs, changed, err := fetcher.fetchIfChanged()
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.changed, changed)

			if !changed {
				require.Nil(t, specs)
			}
		})
	}

	require.NoError(t, os.Remove(path))

	_, changed, err = fetcher.fetchIfChanged()
	require.Error(t, err)
	require.False(t, changed)
}

func writeAKSSettings(t *testing.T, path, data string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(data), 0600))
}
