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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
	. "github.com/onsi/ginkgo/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var agentPoolGVK = schema.GroupVersionKind{
	Group:   "containerservice.azure.com",
	Version: "v1api20240901",
	Kind:    "ManagedClustersAgentPool",
}

// Prepare configures only the cluster selected by this process's binding.
func (e *Environment) Prepare(ctx context.Context) error {
	sets, err := e.azure.listSets(ctx)
	if err != nil {
		return err
	}

	if err := e.findPools(sets); err != nil {
		return err
	}

	if err := e.checkFixture(ctx, sets); err != nil {
		return err
	}

	if err := e.waitForSettlement(ctx); err != nil {
		return err
	}

	if err := e.pinMetricsServer(ctx); err != nil {
		return err
	}

	if err := e.moveAddons(ctx); err != nil {
		return err
	}

	if err := e.freeze(ctx); err != nil {
		return err
	}

	if err := e.createDiskClass(ctx); err != nil {
		return err
	}

	if err := e.prepareGeneral(ctx); err != nil {
		return err
	}

	return e.InstallController(ctx, e.Config.ExpectedImage, false)
}

func (e *Environment) agentPools(ctx context.Context) ([]unstructured.Unstructured, error) {
	var list unstructured.UnstructuredList
	list.SetGroupVersionKind(agentPoolGVK.GroupVersion().WithKind("ManagedClustersAgentPoolList"))
	if err := e.management.List(ctx, &list, client.InNamespace("default")); err != nil {
		return nil, fmt.Errorf("list ASO agent pools: %w", err)
	}

	pools := []unstructured.Unstructured{}

	for _, pool := range list.Items {
		owner, _, err := unstructured.NestedString(pool.Object, "spec", "owner", "name")
		if err != nil {
			return nil, err
		}

		if owner == e.Config.ClusterName {
			pools = append(pools, pool)
		}
	}

	return pools, nil
}

func (e *Environment) checkFixture(ctx context.Context, sets []*armcompute.VirtualMachineScaleSet) error {
	for _, set := range sets {
		pool := ptr.Deref(set.Tags["aks-managed-poolName"], "")
		if set.SKU == nil || set.SKU.Capacity == nil {
			return fmt.Errorf("pool %s has no capacity", pool)
		}

		capacity, min, max := int64(0), "0", "2"

		switch pool {
		case "pool0":
			if *set.SKU.Capacity != 1 || set.Tags["cluster-autoscaler-name"] != nil {
				return fmt.Errorf("system pool is not untagged at one worker")
			}
			continue
		case "main":
			capacity, min, max = 1, "1", "2"
		case "zero":
			max = "1"
		}

		if *set.SKU.Capacity != capacity || ptr.Deref(set.Tags["min"], "") != min ||
			ptr.Deref(set.Tags["max"], "") != max ||
			ptr.Deref(set.Tags["cluster-autoscaler-name"], "") != e.Config.ClusterName {
			return fmt.Errorf("VMSS %s does not match the test fixture", pool)
		}
	}

	pools, err := e.agentPools(ctx)
	if err != nil {
		return err
	}

	if len(pools) != len(sets) {
		return fmt.Errorf("expected %d ASO agent pools, got %d", len(sets), len(pools))
	}

	for _, pool := range pools {
		enabled, _, err := unstructured.NestedBool(pool.Object, "status", "enableAutoScaling")
		if err != nil {
			return fmt.Errorf("ASO agent pool %s has invalid autoscaling state: %v", pool.GetName(), err)
		}

		if enabled {
			return fmt.Errorf("ASO agent pool %s has autoscaling enabled", pool.GetName())
		}
	}

	return nil
}

// settled checks Azure provisioning, Nodes and system Deployments. It returns
// the first unsettled check, or an empty string once everything has settled.
func (e *Environment) settled(ctx context.Context) (string, error) {
	// Require AKS and every pool to finish provisioning.
	c := e.Config
	cluster, err := e.azure.clusters.Get(ctx, c.ClusterName, c.ClusterName, nil)
	if err != nil {
		return "", azureError("read AKS state", err)
	}

	if cluster.Properties == nil {
		return "AKS has no properties", nil
	}

	if state := ptr.Deref(cluster.Properties.ProvisioningState, ""); state != "Succeeded" {
		return fmt.Sprintf("AKS is %q", state), nil
	}

	if n := len(cluster.Properties.AgentPoolProfiles); n != 5 {
		return fmt.Sprintf("AKS has %d of 5 pools", n), nil
	}

	for _, pool := range cluster.Properties.AgentPoolProfiles {
		if pool == nil {
			return "AKS returned an empty pool", nil
		}

		name := ptr.Deref(pool.Name, "")
		if state := ptr.Deref(pool.ProvisioningState, ""); state != "Succeeded" {
			return fmt.Sprintf("pool %s is %q", name, state), nil
		}

		if ptr.Deref(pool.EnableAutoScaling, false) {
			return fmt.Sprintf("pool %s has AKS autoscaling enabled", name), nil
		}
	}

	// Check the VMSS models and their instances.
	sets, err := e.azure.listSets(ctx)
	if err != nil {
		return "", err
	}

	if len(sets) != 5 {
		return fmt.Sprintf("found %d of 5 VMSS", len(sets)), nil
	}

	for _, set := range sets {
		if set == nil || set.Properties == nil || set.Name == nil {
			return "Azure returned an incomplete VMSS", nil
		}

		if state := ptr.Deref(set.Properties.ProvisioningState, ""); state != "Succeeded" {
			return fmt.Sprintf("VMSS %s is %q", *set.Name, state), nil
		}

		pager := e.azure.vms.NewListPager(c.ResourceGroup, *set.Name, nil)

		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return "", azureError("read settling VMSS instances", err)
			}

			for _, vm := range page.Value {
				if vm == nil || vm.Properties == nil {
					return fmt.Sprintf("VMSS %s returned an incomplete instance", *set.Name), nil
				}

				id := ptr.Deref(vm.InstanceID, "")
				if state := ptr.Deref(vm.Properties.ProvisioningState, ""); state != "Succeeded" {
					return fmt.Sprintf("VMSS %s instance %s is %q", *set.Name, id, state), nil
				}

				if !ptr.Deref(vm.Properties.LatestModelApplied, false) {
					return fmt.Sprintf("VMSS %s instance %s is not on the latest model", *set.Name, id), nil
				}
			}
		}
	}

	// Wait for the baseline Nodes and system Deployments.
	nodes, err := e.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Sprintf("Nodes are not observable yet: %v", err), nil
	}

	if len(nodes.Items) != 2 {
		return fmt.Sprintf("found %d of 2 Nodes", len(nodes.Items)), nil
	}

	for _, node := range nodes.Items {
		if !Ready(node) {
			return fmt.Sprintf("Node %s is not Ready", node.Name), nil
		}
	}

	deployments, err := e.Kube.AppsV1().Deployments("kube-system").List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Sprintf("System Deployments are not observable yet: %v", err), nil
	}

	for _, deployment := range deployments.Items {
		replicas := ptr.Deref(deployment.Spec.Replicas, 1)
		if deployment.Status.ObservedGeneration < deployment.Generation ||
			deployment.Status.Replicas != replicas || deployment.Status.UpdatedReplicas != replicas ||
			deployment.Status.AvailableReplicas != replicas {
			return fmt.Sprintf("Deployment %s is not rolled out", deployment.Name), nil
		}
	}

	return "", nil
}

// waitForSettlement allows AKS's post-create work to finish before observation.
func (e *Environment) waitForSettlement(ctx context.Context) error {
	// AKS may still update worker models after reporting Ready.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Minute):
	}

	passes := 0
	reason := ""

	err := wait.PollUntilContextTimeout(
		ctx,
		30*time.Second,
		35*time.Minute,
		true,
		func(ctx context.Context) (bool, error) {
			unsettled, err := e.settled(ctx)
			if err != nil {
				return false, err
			}

			reason = unsettled
			if reason == "" {
				passes++
				Logf("AKS settlement checks: %d/3", passes)
			} else {
				passes = 0
				Logf("AKS settlement checks: 0/3, %s", reason)
			}

			return passes == 3, nil
		},
	)
	if err != nil && reason != "" {
		return fmt.Errorf("AKS did not settle (%s): %w", reason, err)
	}

	return err
}

// pinMetricsServer keeps its requests constant as workers scale.
func (e *Environment) pinMetricsServer(ctx context.Context) error {
	var deployment appsv1.Deployment
	key := client.ObjectKey{Namespace: "kube-system", Name: "metrics-server"}
	if err := e.K8s.Get(ctx, key, &deployment); err != nil {
		return err
	}

	var config string

	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == "metrics-server" {
			cpu, hasCPU := container.Resources.Requests[corev1.ResourceCPU]
			memory, hasMemory := container.Resources.Requests[corev1.ResourceMemory]
			if !hasCPU || !hasMemory {
				return fmt.Errorf("metrics-server requests are missing")
			}

			config = fmt.Sprintf("apiVersion: nannyconfig/v1alpha1\nkind: NannyConfiguration\nbaseCPU: %s\ncpuPerNode: 0m\nbaseMemory: %s\nmemoryPerNode: 0Mi",
				cpu.String(), memory.String())
		}
	}

	if config == "" {
		return fmt.Errorf("metrics-server requests are missing")
	}

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "metrics-server-config", Namespace: "kube-system"}}
	_, err := controllerutil.CreateOrPatch(
		ctx,
		e.K8s,
		cm,
		func() error {
			if cm.Data == nil {
				cm.Data = map[string]string{}
			}

			cm.Data["NannyConfiguration"] = config

			return nil
		},
	)
	if err != nil {
		return err
	}

	before := deployment.DeepCopy()
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = map[string]string{}
	}

	deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
	if err := e.K8s.Patch(ctx, &deployment, client.MergeFrom(before)); err != nil {
		return err
	}

	return e.command(
		ctx,
		"",
		"kubectl",
		"rollout",
		"status",
		"-n",
		"kube-system",
		"deployment/metrics-server",
		"--timeout=10m",
	)
}

// moveAddons drains test workers so only DaemonSets remain on them.
func (e *Environment) moveAddons(ctx context.Context) error {
	var nodes corev1.NodeList
	selector, err := labels.Parse("kubernetes.azure.com/agentpool in (main,zero)")
	if err != nil {
		return err
	}

	if err := e.K8s.List(ctx, &nodes, client.MatchingLabelsSelector{
		Selector: selector,
	}); err != nil {
		return err
	}

	if len(nodes.Items) == 0 {
		return fmt.Errorf("the main pool has no Node")
	}

	for i := range nodes.Items {
		before := nodes.Items[i].DeepCopy()
		nodes.Items[i].Spec.Unschedulable = true
		if err := e.K8s.Patch(ctx, &nodes.Items[i], client.MergeFrom(before)); err != nil {
			return err
		}
	}

	for _, node := range nodes.Items {
		if err := e.command(
			ctx,
			"",
			"kubectl",
			"drain",
			node.Name,
			"--ignore-daemonsets",
			"--delete-emptydir-data",
			"--timeout=10m",
		); err != nil {
			return err
		}
	}

	if err := wait.PollUntilContextTimeout(ctx, 10*time.Second, 10*time.Minute, true, e.systemPodsReady); err != nil {
		return fmt.Errorf("system Pods did not become Ready after draining: %w", err)
	}

	for _, node := range nodes.Items {
		var current corev1.Node

		if err := e.K8s.Get(ctx, client.ObjectKeyFromObject(&node), &current); err != nil {
			return err
		}

		before := current.DeepCopy()
		current.Spec.Unschedulable = false
		if err := e.K8s.Patch(ctx, &current, client.MergeFrom(before)); err != nil {
			return err
		}
	}

	return nil
}

func (e *Environment) systemPodsReady(ctx context.Context) (bool, error) {
	var pods corev1.PodList

	if err := e.K8s.List(ctx, &pods, client.InNamespace("kube-system")); err != nil {
		Logf("System Pods are not observable yet: %v", err)

		return false, nil
	}

	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}

		if pod.Spec.NodeName == "" {
			return false, nil
		}

		if !PodReady(pod) {
			return false, nil
		}
	}

	return true, nil
}

// freeze pauses CAPI and skips ASO reconciliation for the bound cluster.
func (e *Environment) freeze(ctx context.Context) error {
	cluster := &unstructured.Unstructured{}
	cluster.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cluster.x-k8s.io",
		Version: "v1beta1",
		Kind:    "Cluster",
	})
	if err := e.annotateOwned(ctx, cluster, e.Config.ClusterName, "cluster.x-k8s.io/paused", "true"); err != nil {
		return err
	}

	managed := &unstructured.Unstructured{}
	managed.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "containerservice.azure.com",
		Version: "v1api20240901",
		Kind:    "ManagedCluster",
	})
	if err := e.annotateOwned(ctx, managed, e.Config.ClusterName, "serviceoperator.azure.com/reconcile-policy", "skip"); err != nil {
		return err
	}

	pools, err := e.agentPools(ctx)
	if err != nil {
		return err
	}

	for i := range pools {
		if err := e.annotateOwned(ctx, &pools[i], pools[i].GetName(), "serviceoperator.azure.com/reconcile-policy", "skip"); err != nil {
			return err
		}
	}

	return nil
}

func (e *Environment) annotateOwned(ctx context.Context, object *unstructured.Unstructured, name, key, value string) error {
	if err := e.management.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, object); err != nil {
		return err
	}

	before := object.DeepCopy()
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	annotations[key] = value
	object.SetAnnotations(annotations)

	return e.management.Patch(ctx, object, client.MergeFrom(before))
}

func repoFile(path string) string {
	_, file, _, _ := runtime.Caller(0)

	return filepath.Join(filepath.Dir(file), "../../..", path)
}

func (e *Environment) command(ctx context.Context, input, name string, args ...string) error {
	Logf("%s %s", name, strings.Join(args, " "))
	command := exec.CommandContext(ctx, name, args...)
	command.Env = append(os.Environ(), "KUBECONFIG="+e.Config.Kubeconfig)
	command.Stdin = strings.NewReader(input)
	command.Stdout, command.Stderr = GinkgoWriter, GinkgoWriter
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}

	return nil
}
