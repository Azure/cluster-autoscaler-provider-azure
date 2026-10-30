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
	"context"
	"fmt"
	"strings"
	"text/template"
)

type controllerValues struct {
	ClientID        string
	SubscriptionID  string
	ResourceGroup   string
	ClusterName     string
	ImageRepository string
	ImageTag        string
}

// InstallController installs the local chart with CAPZ's Helm CLI.
func (e *Environment) InstallController(ctx context.Context, image string) error {
	colon := strings.LastIndex(image, ":")
	if colon < 0 {
		return fmt.Errorf("controller image must include a tag")
	}

	data := controllerValues{
		ClientID:        e.Config.KubeletClientID,
		SubscriptionID:  e.Config.SubscriptionID,
		ResourceGroup:   e.Config.ResourceGroup,
		ClusterName:     e.Config.ClusterName,
		ImageRepository: image[:colon],
		ImageTag:        image[colon+1:],
	}
	var values strings.Builder

	if err := controllerValuesTemplate.Execute(&values, data); err != nil {
		return fmt.Errorf("render controller values: %w", err)
	}

	return e.command(
		ctx,
		values.String(),
		"helm",
		"upgrade",
		"--install",
		"cluster-autoscaler",
		repoFile("charts/cluster-autoscaler"),
		"--namespace",
		"default",
		"--wait",
		"--timeout=5m",
		"--values",
		"/dev/stdin",
	)
}

var controllerValuesTemplate = template.Must(template.New("autoscaler-values").Parse(`azureUseManagedIdentityExtension: true
azureUserAssignedIdentityID: {{ .ClientID }}
azureSubscriptionID: {{ .SubscriptionID }}
azureResourceGroup: {{ .ResourceGroup }}
autoDiscovery:
  clusterName: {{ .ClusterName }}
replicaCount: 1
image:
  repository: {{ .ImageRepository }}
  tag: "{{ .ImageTag }}"
  pullPolicy: Always
nodeSelector:
  kubernetes.io/os: linux
  kubernetes.azure.com/agentpool: pool0
resources:
  requests: {cpu: 100m, memory: 256Mi}
  limits: {cpu: 500m, memory: 512Mi}
extraArgs:
  v: 4
  write-status-configmap: true
  leader-elect: true
  scale-down-delay-after-add: 30s
  scale-down-unneeded-time: 30s
  scale-down-delay-after-delete: 0s
  unremovable-node-recheck-timeout: 30s
  max-node-provision-time: 5m
  max-nodes-total: 4
  scan-interval: 10s
  skip-nodes-with-local-storage: true
  skip-nodes-with-system-pods: true
  max-graceful-termination-sec: 120
  expendable-pods-priority-cutoff: -10
  bypassed-scheduler-names: non-existing-bypassed-scheduler
updateStrategy:
  type: Recreate
`))
