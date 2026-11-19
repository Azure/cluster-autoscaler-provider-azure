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
	Deallocate      bool
}

// InstallController installs the local chart with CAPZ's Helm CLI.
func (e *Environment) InstallController(ctx context.Context, image string, deallocate bool) error {
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
		Deallocate:      deallocate,
	}
	var values strings.Builder

	if err := controllerValuesTemplate.Execute(&values, data); err != nil {
		return fmt.Errorf("render controller values: %w", err)
	}

	e.SaveControllerLogs(ctx)

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
rbac:
  additionalRules:
    - apiGroups: [""]
      resources: ["nodes/status"]
      verbs: ["update"]
extraEnv:
  CLUSTER_NAME: {{ .ClusterName }}
  ARM_CLUSTER_RESOURCE_GROUP: {{ .ClusterName }}
  AZURE_DEALLOCATE: "{{ .Deallocate }}"
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
  balance-similar-node-groups: true
  enforce-node-group-min-size: true
  balancing-ignore-label_1: agentpool
  balancing-ignore-label_2: kubernetes.azure.com/agentpool
  balancing-ignore-label_3: aks-managed-poolName
  balancing-ignore-label_4: aks-managed-creationSource
  balancing-ignore-label_5: aks-managed-createOperationID
  balancing-ignore-label_6: aks-managed-consolidated-additional-properties
  startup-taint_1: node.cloudprovider.kubernetes.io/shutdown
  startup-taint_2: node.kubernetes.io/out-of-service
updateStrategy:
  type: Recreate
`))
