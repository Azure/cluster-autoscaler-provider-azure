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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	clusterNamePattern = regexp.MustCompile(`^e2ea[a-z0-9]{1,32}$`)
	locationPattern    = regexp.MustCompile(`^[a-z0-9]+$`)
	guidPattern        = regexp.MustCompile(`^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$`)
)

// Config is the JSON binding to one disposable cluster. The test README
// describes each field.
type Config struct {
	Kubeconfig           string `json:"kubeconfig"`
	ManagementKubeconfig string `json:"managementKubeconfig"`
	ClusterName          string `json:"clusterName"`
	SubscriptionID       string `json:"subscriptionID"`
	ResourceGroup        string `json:"resourceGroup"`
	KubeletClientID      string `json:"kubeletClientID"`
	ExpectedImage        string `json:"expectedImage"`

	// These values are discovered or set by this process, never supplied in JSON.
	RunID    string `json:"-"`
	MainPool string `json:"-"`
	ZeroPool string `json:"-"`
}

// LoadConfig reads one JSON object from path, rejects unknown fields and
// validates the result.
func LoadConfig(path string) (Config, error) {
	var cfg Config

	if err := loadConfigFile(path, &cfg); err != nil {
		return cfg, err
	}

	cfg.RunID = cfg.ClusterName

	return cfg, cfg.Validate()
}

// LoadConfigForProcess selects one distinct cluster per parallel process.
// Serial runs retain the single-object binding.
func LoadConfigForProcess(path string, process, total int) (Config, error) {
	if total < 1 || process < 1 || process > total {
		return Config{}, fmt.Errorf("invalid parallel process %d of %d", process, total)
	}

	if total == 1 {
		return LoadConfig(path)
	}

	configs := []Config{}
	if err := loadConfigFile(path, &configs); err != nil {
		return Config{}, err
	}

	if len(configs) != total {
		return Config{}, fmt.Errorf("got %d cluster bindings for %d parallel processes", len(configs), total)
	}

	kubeconfigs := map[string]bool{}
	runs := map[string]bool{}

	for i := range configs {
		configs[i].RunID = configs[i].ClusterName
		cfg := configs[i]
		if err := cfg.Validate(); err != nil {
			return Config{}, fmt.Errorf("cluster binding %d: %w", i+1, err)
		}

		kubeconfig := filepath.Clean(cfg.Kubeconfig)
		if kubeconfigs[kubeconfig] || runs[cfg.RunID] {
			return Config{}, fmt.Errorf("parallel cluster bindings must have distinct kubeconfigs and run IDs")
		}

		kubeconfigs[kubeconfig], runs[cfg.RunID] = true, true
	}

	return configs[process-1], nil
}

func loadConfigFile(path string, target any) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open environment file: %w", err)
	}

	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode environment file: %w", err)
	}

	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("environment file must contain exactly one JSON value")
	}

	return nil
}

// Validate returns an error for a missing or malformed field.
func (c Config) Validate() error {
	required := map[string]string{
		"kubeconfig":           c.Kubeconfig,
		"managementKubeconfig": c.ManagementKubeconfig,
		"clusterName":          c.ClusterName,
		"subscriptionID":       c.SubscriptionID,
		"resourceGroup":        c.ResourceGroup,
		"kubeletClientID":      c.KubeletClientID,
		"expectedImage":        c.ExpectedImage,
	}

	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}

	if !filepath.IsAbs(c.Kubeconfig) || !filepath.IsAbs(c.ManagementKubeconfig) {
		return fmt.Errorf("kubeconfigs must be absolute file paths")
	}

	if !clusterNamePattern.MatchString(c.ClusterName) {
		return fmt.Errorf("clusterName must be the run's e2ea-prefixed cluster")
	}

	prefix := strings.ToLower("MC_" + c.ClusterName + "_" + c.ClusterName + "_")
	if !strings.HasPrefix(strings.ToLower(c.ResourceGroup), prefix) ||
		!locationPattern.MatchString(strings.TrimPrefix(strings.ToLower(c.ResourceGroup), prefix)) {
		return fmt.Errorf("resourceGroup must be this cluster's AKS node resource group")
	}

	for _, id := range []string{c.SubscriptionID, c.KubeletClientID} {
		if !guidPattern.MatchString(id) {
			return fmt.Errorf("subscriptionID and kubeletClientID must be GUIDs")
		}
	}

	for _, image := range []string{c.ExpectedImage} {
		if image != "" && (strings.ContainsAny(image, "\r\n\"") || !strings.Contains(image, ":")) {
			return fmt.Errorf("images must use a tagged reference")
		}
	}

	return nil
}

// Pools returns the VMSS names observed by this run.
func (c Config) Pools() []string {
	pools := []string{c.MainPool, c.ZeroPool}

	return pools
}

// PoolID returns the Azure resource ID of the VMSS name.
func (c Config) PoolID(name string) string {
	return "/subscriptions/" + c.SubscriptionID + "/resourceGroups/" + c.ResourceGroup +
		"/providers/Microsoft.Compute/virtualMachineScaleSets/" + name
}

func normalizeID(id string) string {
	return strings.ToLower(strings.TrimPrefix(id, "azure://"))
}
