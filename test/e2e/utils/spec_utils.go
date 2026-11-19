/*
Copyright 2024 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// SettleTimeout allows Azure capacity and Nodes to converge.
	SettleTimeout = 15 * time.Minute
	// PollInterval is the interval between live observations.
	PollInterval = 10 * time.Second
	// ControllerTimeout covers Pod start, lease takeover and the first loop.
	ControllerTimeout = 3 * time.Minute
)

var (
	// Env is the cluster bound to this parallel process.
	Env *Environment
	// Namespace contains this spec's workloads.
	Namespace *corev1.Namespace
)

// SetupSpec checks the controller and baseline, then creates a test namespace.
func SetupSpec(ctx SpecContext) {
	Eventually(ctx, Env.Controller, ControllerTimeout, PollInterval).Should(Succeed())

	AddReportEntry("runtime-image", Env.Config.ExpectedImage)

	AddReportEntry("cluster-binding", fmt.Sprintf(
		"process=%d runID=%s resourceGroup=%s",
		GinkgoParallelProcess(),
		Env.Config.RunID,
		Env.Config.ResourceGroup,
	))
	baseline := WaitStable(ctx, 1, 0)

	Eventually(
		ctx,
		func() error {
			return Env.CheckWorkerIsolation(ctx, baseline)
		},
		time.Minute,
		PollInterval,
	).Should(Succeed())
	Namespace = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		GenerateName: "azure-e2e-", Labels: map[string]string{RunLabel: Env.Config.RunID},
	}}
	Expect(Env.K8s.Create(ctx, Namespace)).To(Succeed())
	created := Namespace.DeepCopy()

	DeferCleanup(func(ctx SpecContext) {
		if CurrentSpecReport().Failed() {
			var events corev1.EventList
			if err := Env.K8s.List(ctx, &events, client.InNamespace(created.Name)); err != nil {
				fmt.Fprintf(GinkgoWriter, "Could not list namespace events: %v\n", err)
			} else {
				for _, event := range events.Items {
					fmt.Fprintf(GinkgoWriter, "%s/%s %s: %s\n", event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Reason, event.Message)
				}
			}
		}

		Expect(client.IgnoreNotFound(Env.K8s.Delete(ctx, created))).To(Succeed())

		Eventually(
			ctx,
			func() error {
				return Env.K8s.Get(ctx, client.ObjectKeyFromObject(created), &corev1.Namespace{})
			},
			4*time.Minute,
			PollInterval,
		).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
		WaitStable(ctx, 1, 0)
	}, NodeTimeout(20*time.Minute))
}

// ReadSnapshot reads the environment and stops the enclosing poll at once
// when a pool is above its max tag.
func ReadSnapshot(ctx context.Context) (Snapshot, error) {
	snapshot, err := Env.Read(ctx)
	if errors.Is(err, ErrBounds) {
		StopTrying("a pool exceeded its maximum").Wrap(err).Now()
	}

	return snapshot, err
}

// ReadActiveSnapshot checks the running controller, then works like
// ReadSnapshot.
func ReadActiveSnapshot(ctx context.Context) (Snapshot, error) {
	if err := Env.Controller(ctx); err != nil {
		return Snapshot{}, err
	}

	return ReadSnapshot(ctx)
}

// WaitStable waits until main and zero have the given number of Ready
// workers and matching Azure capacity.
func WaitStable(ctx context.Context, main, zero int) Snapshot {
	var snapshot Snapshot

	Eventually(
		ctx,
		func() error {
			var err error
			snapshot, err = ReadSnapshot(ctx)
			if err != nil {
				return err
			}

			return snapshot.Stable(Env.Config, main, zero)
		},
		SettleTimeout,
		PollInterval,
	).Should(Succeed())
	ReportSnapshot(fmt.Sprintf("stable-%d-%d", main, zero), snapshot)

	return snapshot
}

// ReportSnapshot adds the pools and worker provider IDs of snapshot to the
// report.
func ReportSnapshot(name string, snapshot Snapshot) {
	nodes := map[string]string{}

	for _, node := range WorkerNodes(snapshot.Nodes, Env.Config) {
		nodes[node.Name] = node.Spec.ProviderID
	}

	AddReportEntry(name, struct {
		Pools           map[string]PoolState
		NodeProviderIDs map[string]string
	}{snapshot.Pools, nodes})
}

// WaitWorkload waits for the workload state in the test Namespace. See
// Environment.WorkloadState.
func WaitWorkload(ctx context.Context, name, pool string, ready, pending int) []corev1.Pod {
	return WaitWorkloadIn(ctx, Namespace.Name, name, pool, ready, pending)
}

// WaitWorkloadIn works like WaitWorkload in workloadNamespace.
func WaitWorkloadIn(ctx context.Context, workloadNamespace, name, pool string, ready, pending int) []corev1.Pod {
	var pods []corev1.Pod

	Eventually(
		ctx,
		func() error {
			var err error
			pods, err = Env.WorkloadState(ctx, workloadNamespace, name, pool, ready, pending)

			return err
		},
		SettleTimeout,
		PollInterval,
	).Should(Succeed())

	return pods
}

// WaitDeleted waits until main and zero are stable at the given counts and
// deleted instances of pool in before are physically gone.
func WaitDeleted(ctx context.Context, before Snapshot, pool string, deleted, main, zero int) {
	var after Snapshot

	Eventually(
		ctx,
		func() error {
			var err error
			after, err = ReadSnapshot(ctx)
			if err != nil {
				return err
			}

			if err := after.Stable(Env.Config, main, zero); err != nil {
				return err
			}

			return Env.Deleted(ctx, before, after, pool, deleted)
		},
		SettleTimeout,
		PollInterval,
	).Should(Succeed())
	ReportSnapshot("physical-delete-survivors", after)

	AddReportEntry("physical-delete", fmt.Sprintf("pool=%s removedInstances=%d; captured instance, Node and NIC IDs absent", pool, deleted))
}

// ScaleDeployment sets the replica count of workload with a merge patch.
func ScaleDeployment(ctx context.Context, workload *appsv1.Deployment, replicas int32) {
	original := workload.DeepCopy()
	workload.Spec.Replicas = ptr.To(replicas)
	Expect(Env.K8s.Patch(ctx, workload, client.MergeFrom(original))).To(Succeed())
}
