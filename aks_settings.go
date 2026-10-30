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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
)

type aksNodeGroup struct {
	Name            string            `json:"name"`
	MinSize         int               `json:"minSize"`
	MaxSize         int               `json:"maxSize"`
	ScaleDownPolicy string            `json:"scaleDownPolicy"`
	Labels          map[string]string `json:"labels"`
	Taints          string            `json:"taints"`
}

type aksSettings struct {
	NodeGroups []aksNodeGroup `json:"nodeGroups"`
}

func decodeAKSSettings(data []byte) (aksSettings, error) {
	var settings aksSettings
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096).Decode(&settings); err != nil {
		return aksSettings{}, fmt.Errorf("failed to decode autoscaler settings: %w", err)
	}

	for i := range settings.NodeGroups {
		group := &settings.NodeGroups[i]
		if group.ScaleDownPolicy == "" {
			group.ScaleDownPolicy = "Delete"
		}

		if group.Name == "" {
			return aksSettings{}, fmt.Errorf("invalid node group: name must not be blank")
		}
		if group.MaxSize < group.MinSize {
			return aksSettings{}, fmt.Errorf("invalid node group %q: max size must be at least min size", group.Name)
		}
		if group.ScaleDownPolicy != "Delete" && group.ScaleDownPolicy != "Deallocate" {
			return aksSettings{}, fmt.Errorf("invalid node group %q: scaleDownPolicy %q is not supported; use Delete or Deallocate",
				group.Name, group.ScaleDownPolicy)
		}
	}

	// The fork normalizes an empty group list to nil before comparing settings.
	if len(settings.NodeGroups) == 0 {
		settings.NodeGroups = nil
	}
	return settings, nil
}

func (s aksSettings) nodeGroupSpecs() ([]string, error) {
	result := make([]string, 0, len(s.NodeGroups))
	for _, group := range s.NodeGroups {
		labels := group.Labels
		if len(labels) == 0 {
			labels = map[string]string{}
		}
		encodedLabels, err := json.Marshal(labels)
		if err != nil {
			return nil, fmt.Errorf("failed to encode labels for node group %q: %w", group.Name, err)
		}
		result = append(result, fmt.Sprintf(
			"%d:%d:%s:%s:%s|%s",
			group.MinSize, group.MaxSize, group.ScaleDownPolicy, group.Name, encodedLabels, group.Taints,
		))
	}
	return result, nil
}

type aksSettingsFetcher struct {
	path     string
	previous aksSettings
}

func newAKSSettingsFetcher(path string) *aksSettingsFetcher {
	return &aksSettingsFetcher{path: path}
}

// fetchIfChanged compares decoded settings with the last successful read.
func (f *aksSettingsFetcher) fetchIfChanged() ([]string, bool, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read settings file %q: %w", f.path, err)
	}

	settings, err := decodeAKSSettings(data)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load settings file %q: %w", f.path, err)
	}

	if reflect.DeepEqual(f.previous, settings) {
		return nil, false, nil
	}

	specs, err := settings.nodeGroupSpecs()
	if err != nil {
		return nil, false, err
	}

	f.previous = settings
	return specs, true, nil
}

// useAKSSettings replaces explicit groups and keeps watching after a failed read.
func useAKSSettings(opts *config.AutoscalingOptions, path string) (*aksSettingsFetcher, error) {
	if path == "" {
		return nil, nil
	}

	fetcher := newAKSSettingsFetcher(path)
	opts.NodeGroups = []string{}
	specs, changed, err := fetcher.fetchIfChanged()
	if err != nil {
		return fetcher, err
	}

	if changed {
		opts.NodeGroups = specs
	}
	return fetcher, nil
}
