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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testConfig() Config {
	return Config{
		Kubeconfig:           "/explicit/kubeconfig",
		ManagementKubeconfig: "/explicit/management",
		ClusterName:          "e2eatest",
		RunID:                "e2eatest",
		SubscriptionID:       "00000000-0000-0000-0000-000000000001",
		ResourceGroup:        "MC_e2eatest_e2eatest_eastus2",
		KubeletClientID:      "00000000-0000-0000-0000-000000000002",
		ExpectedImage:        "candidate:tag",
		MainPool:             "main",
		ZeroPool:             "zero",
	}
}

func TestLoadConfigForProcess(t *testing.T) {
	t.Parallel()
	first, second := testConfig(), testConfig()
	second.Kubeconfig, second.ClusterName, second.RunID = "/second/kubeconfig", "e2easecond", "e2easecond"
	second.ResourceGroup = "MC_e2easecond_e2easecond_eastus2"
	valid, err := json.Marshal([]Config{first, second})
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name    string
		body    any
		raw     string
		process int
		total   int
		want    string
	}{
		{
			name:    "serial compatibility",
			body:    first,
			process: 1,
			total:   1,
			want:    first.RunID,
		},
		{
			name:    "first cluster",
			body:    []Config{first, second},
			process: 1,
			total:   2,
			want:    first.RunID,
		},
		{
			name:    "second cluster",
			body:    []Config{first, second},
			process: 2,
			total:   2,
			want:    second.RunID,
		},
		{
			name:    "single shared binding",
			body:    first,
			process: 1,
			total:   2,
		},
		{
			name:    "too few clusters",
			body:    []Config{first},
			process: 1,
			total:   2,
		},
		{
			name: "too many clusters",
			body: []Config{
				first,
				second,
				first,
			},
			process: 1,
			total:   2,
		},
		{
			name:    "duplicate cluster",
			body:    []Config{first, first},
			process: 1,
			total:   2,
		},
		{
			name:    "invalid process",
			body:    first,
			process: 0,
			total:   1,
		},
		{
			name:    "process beyond total",
			body:    first,
			process: 2,
			total:   1,
		},
		{
			name:    "invalid total",
			body:    first,
			process: 1,
			total:   0,
		},
		{
			name:    "unknown array field",
			raw:     strings.Replace(string(valid), `"kubeconfig":`, `"unexpected":true,"kubeconfig":`, 1),
			process: 1,
			total:   2,
		},
		{
			name:    "trailing JSON",
			raw:     string(valid) + " {}",
			process: 1,
			total:   2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "environment.json")
			body, err := json.Marshal(tt.body)
			if err != nil {
				t.Fatal(err)
			}

			if tt.raw != "" {
				body = []byte(tt.raw)
			}

			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}

			cfg, err := LoadConfigForProcess(path, tt.process, tt.total)
			if tt.want == "" {
				if err == nil {
					t.Fatal("invalid parallel binding accepted")
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if cfg.RunID != tt.want {
				t.Fatalf("selected %q, want %q", cfg.RunID, tt.want)
			}
		})
	}

	for _, tt := range []struct {
		name   string
		change func(*Config)
	}{
		{name: "duplicate kubeconfig", change: func(c *Config) { c.Kubeconfig = first.Kubeconfig }},
		{name: "duplicate run ID", change: func(c *Config) {
			c.ClusterName = first.ClusterName
			c.ResourceGroup = first.ResourceGroup
		}},
		{name: "invalid unselected cluster", change: func(c *Config) { c.ExpectedImage = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			other := second
			tt.change(&other)
			body, err := json.Marshal([]Config{first, other})
			if err != nil {
				t.Fatal(err)
			}

			path := filepath.Join(t.TempDir(), "environment.json")
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}

			if _, err := LoadConfigForProcess(path, 1, 2); err == nil {
				t.Fatal("invalid parallel binding accepted")
			}
		})
	}
}
