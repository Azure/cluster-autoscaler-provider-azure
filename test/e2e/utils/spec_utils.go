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
	"slices"
	"time"

	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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

// ListWorkload lists the Pods of workload name in the test Namespace.
func ListWorkload(ctx context.Context, name string) ([]corev1.Pod, error) {
	return ListWorkloadIn(ctx, Namespace.Name, name)
}

// ListWorkloadIn lists the Pods of workload name in workloadNamespace.
func ListWorkloadIn(ctx context.Context, workloadNamespace, name string) ([]corev1.Pod, error) {
	return Env.workloadPods(ctx, workloadNamespace, name)
}

// AssertDistinctNodes requires pods to run on count different Nodes.
func AssertDistinctNodes(pods []corev1.Pod, count int) {
	nodes := map[string]bool{}

	for _, pod := range pods {
		nodes[pod.Spec.NodeName] = true
	}

	Expect(nodes).To(HaveLen(count), "Pods must run on %d different Nodes", count)
}

// ScaleDeployment sets the replica count of workload with a merge patch.
func ScaleDeployment(ctx context.Context, workload *appsv1.Deployment, replicas int32) {
	original := workload.DeepCopy()
	workload.Spec.Replicas = ptr.To(replicas)
	Expect(Env.K8s.Patch(ctx, workload, client.MergeFrom(original))).To(Succeed())
}

// AntiAffinityDeployment returns a Deployment whose Pods must run on
// different Nodes.
func AntiAffinityDeployment(name, pool string, replicas int32) *appsv1.Deployment {
	workload := Env.Deployment(Namespace.Name, name, pool, "10m", replicas)
	if workload.Spec.Template.Spec.Affinity == nil {
		workload.Spec.Template.Spec.Affinity = &corev1.Affinity{}
	}

	workload.Spec.Template.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey: corev1.LabelHostname, LabelSelector: workload.Spec.Selector.DeepCopy(),
		}},
	}

	return workload
}

// GrowThree starts three anti-affinity Pods and waits until they run on
// three workers. It returns the Deployment and the grown snapshot.
func GrowThree(ctx context.Context) (*appsv1.Deployment, Snapshot) {
	By("growing to three workers with anti-affinity Pods")
	workload := AntiAffinityDeployment("grow-three", "", 3)
	Expect(Env.K8s.Create(ctx, workload)).To(Succeed())
	grown := WaitWorkers(ctx, 3)
	AssertDistinctNodes(WaitWorkload(ctx, workload.Name, "", 3, 0), 3)

	return workload, grown
}

// WaitWorkers waits until main and zero have count workers in total and
// both pools are stable.
func WaitWorkers(ctx context.Context, count int) Snapshot {
	var snapshot Snapshot

	Eventually(
		ctx,
		func() error {
			var err error
			snapshot, err = ReadSnapshot(ctx)
			if err != nil {
				return err
			}

			main, zero := snapshot.Pools[Env.Config.MainPool].Capacity, snapshot.Pools[Env.Config.ZeroPool].Capacity
			if main+zero != count {
				return fmt.Errorf("observed %d target workers, want %d", main+zero, count)
			}

			return snapshot.Stable(Env.Config, main, zero)
		},
		SettleTimeout,
		PollInterval,
	).Should(Succeed())
	ReportSnapshot("stable-workers", snapshot)

	return snapshot
}

// MemoryGeometry returns fraction of the memory that the one main worker can
// allocate. With mustFit, it requires that request and a small growth Pod to
// fit beside the worker's current requests. With a fraction above one half,
// it requires two such requests not to fit on one worker.
func MemoryGeometry(ctx context.Context, fraction float64, mustFit bool) int64 {
	snapshot := WaitStable(ctx, 1, 0)
	node := PoolNodes(snapshot.Nodes, Env.Config.PoolID(Env.Config.MainPool))[0]
	allocatable := node.Status.Allocatable.Memory().Value()
	Expect(allocatable).To(BeNumerically(">", 0), "worker %s must report allocatable memory", node.Name)
	memory := int64(float64(allocatable) * fraction)
	var pods corev1.PodList
	Expect(Env.K8s.List(ctx, &pods)).To(Succeed())
	requests, err := NodeRequests(node, pods.Items)
	Expect(err).NotTo(HaveOccurred())
	occupied := requests.Memory().Value()
	Expect(occupied).To(BeNumerically(">", 0), "the source small-Pod geometry requires measured background requests")
	if mustFit {
		Expect(memory+16*1024*1024).To(BeNumerically("<=", allocatable-occupied), "one reservation plus the growth fixture must fit")
	}

	if fraction > 0.5 {
		Expect(2*memory).To(BeNumerically(">", allocatable), "two reservations must not fit on a worker")
	}

	AddReportEntry("memory-geometry", fmt.Sprintf(
		"worker=%s allocatableBytes=%d existingRequestsBytes=%d requestBytes=%d",
		node.Name,
		allocatable,
		occupied,
		memory,
	))

	return memory
}

// MemoryDeployment returns a Deployment whose Pods request bytes of memory.
func MemoryDeployment(name, pool string, replicas int32, bytes int64) *appsv1.Deployment {
	workload := Env.Deployment(Namespace.Name, name, pool, "5m", replicas)
	workload.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = *resource.NewQuantity(bytes, resource.BinarySI)

	return workload
}

// WaitPodEvent waits for an event with reason on the exact Pod UID.
func WaitPodEvent(ctx context.Context, pod corev1.Pod, reason string) {
	Eventually(
		ctx,
		func() error {
			var events corev1.EventList

			if err := Env.K8s.List(ctx, &events, client.InNamespace(pod.Namespace)); err != nil {
				return err
			}

			for _, event := range events.Items {
				if event.InvolvedObject.UID == pod.UID && event.InvolvedObject.Kind == "Pod" && event.Reason == reason {
					return nil
				}
			}

			return fmt.Errorf("no %s event for test Pod %s with UID %s", reason, pod.Name, pod.UID)
		},
		5*time.Minute,
		PollInterval,
	).Should(Succeed())
}

// ObserveNoGrowth requires the baseline pools and the workload state to stay
// unchanged for five minutes.
func ObserveNoGrowth(ctx context.Context, name, pool string, ready, pending int) {
	Consistently(
		ctx,
		func() error {
			snapshot, err := ReadActiveSnapshot(ctx)
			if err != nil {
				return err
			}

			if err := snapshot.Ready(Env.Config, 1, 0); err != nil {
				return err
			}

			_, err = Env.WorkloadState(ctx, Namespace.Name, name, pool, ready, pending)

			return err
		},
		5*time.Minute,
		PollInterval,
	).Should(Succeed())
}

// WaitBaselineDeleted waits until the pools return to one main worker and
// every worker added since the baseline is physically deleted. When protected
// is not nil, the spec fails at once if fewer than minimumReady of its Pods
// are Ready.
func WaitBaselineDeleted(ctx context.Context, before Snapshot, minimumReady int, protected *appsv1.Deployment) {
	var after Snapshot

	Eventually(
		ctx,
		func() error {
			if protected != nil {
				pods, err := ListWorkloadIn(ctx, protected.Namespace, protected.Name)
				if err != nil {
					return err
				}

				ready := 0

				for _, pod := range pods {
					if PodReady(pod) {
						ready++
					}
				}

				if ready < minimumReady {
					StopTrying(fmt.Sprintf(
						"workload %s had %d Ready Pods during the drain, want at least %d",
						protected.Name,
						ready,
						minimumReady,
					)).Now()
				}
			}

			var err error
			after, err = ReadSnapshot(ctx)
			if err != nil {
				return err
			}

			if err := after.Stable(Env.Config, 1, 0); err != nil {
				return err
			}

			for pool, baseline := range map[string]int{Env.Config.MainPool: 1, Env.Config.ZeroPool: 0} {
				if err := Env.Deleted(ctx, before, after, pool, len(before.Pools[pool].Instances)-baseline); err != nil {
					return err
				}
			}

			return nil
		},
		SettleTimeout,
		PollInterval,
	).Should(Succeed())
	ReportSnapshot("physical-delete-baseline", after)
}

// Drain grows to three workers, spreads a PDB-protected workload across them
// and removes the growth demand. With allowed set to zero, it requires the
// workers to stay. Otherwise it waits for the added workers to be deleted
// while the PDB keeps the protected Pods available.
func Drain(ctx context.Context, podsPerNode, allowed int, workloadNamespace string) {
	growth, grown := GrowThree(ctx)

	By("spreading a PDB-protected workload across the workers")
	replicas := int32(3 * podsPerNode)
	protected := Env.Deployment(workloadNamespace, "drain-"+Namespace.Name, "", "10m", replicas)
	protected.Spec.Template.Spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
			Weight: 100, PodAffinityTerm: corev1.PodAffinityTerm{TopologyKey: corev1.LabelHostname, LabelSelector: protected.Spec.Selector.DeepCopy()},
		}},
	}
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      protected.Name,
			Namespace: workloadNamespace,
			Labels:    protected.Labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: ptr.To(intstr.FromInt32(replicas - int32(allowed))),
			Selector:     protected.Spec.Selector.DeepCopy(),
		},
	}
	Expect(Env.K8s.Create(ctx, pdb)).To(Succeed())
	if workloadNamespace == "kube-system" {
		cleanupSystemObject(pdb)
	}

	Expect(Env.K8s.Create(ctx, protected)).To(Succeed())
	if workloadNamespace == "kube-system" {
		cleanupSystemObject(protected)
	}

	pods := WaitWorkloadIn(ctx, workloadNamespace, protected.Name, "", int(replicas), 0)
	distribution := map[string]int{}

	for _, pod := range pods {
		distribution[pod.Spec.NodeName]++
	}

	Expect(distribution).To(HaveLen(3), "drain fixture must start with movable replicas on every worker")

	for node, count := range distribution {
		Expect(count).To(
			Equal(podsPerNode),
			"drain fixture must start with %d movable replicas on worker %s",
			podsPerNode,
			node,
		)
	}

	Eventually(
		ctx,
		func(g Gomega) {
			g.Expect(Env.K8s.Get(ctx, client.ObjectKeyFromObject(pdb), pdb)).To(Succeed())
			g.Expect(pdb.Status.ObservedGeneration).To(Equal(pdb.Generation))
			g.Expect(pdb.Status.DisruptionsAllowed).To(Equal(int32(allowed)))
		},
		time.Minute,
		PollInterval,
	).Should(Succeed())

	By("removing the growth demand")
	Expect(Env.K8s.Delete(ctx, growth)).To(Succeed())
	if allowed == 0 {
		By("checking that the PDB keeps every worker for five minutes")

		ObserveKept(ctx, grown, workloadNamespace, protected.Name, int(replicas))

		return
	}

	By("waiting for the drained workers to be deleted")
	WaitBaselineDeleted(ctx, grown, int(replicas)-allowed, protected)
	WaitWorkloadIn(ctx, workloadNamespace, protected.Name, Env.Config.MainPool, int(replicas), 0)
}

func cleanupSystemObject(object client.Object) {
	DeferCleanup(func(ctx SpecContext) {
		Expect(client.IgnoreNotFound(Env.K8s.Delete(ctx, object))).To(Succeed())
		if deployment, ok := object.(*appsv1.Deployment); ok {
			Eventually(
				ctx,
				func() ([]corev1.Pod, error) {
					return ListWorkloadIn(ctx, deployment.Namespace, deployment.Name)
				},
				4*time.Minute,
				PollInterval,
			).Should(BeEmpty())
		}
	}, NodeTimeout(5*time.Minute))
}

// ControllerContainer returns the autoscaler container from the Deployment
// template.
func ControllerContainer(ctx context.Context) corev1.Container {
	var deployment appsv1.Deployment
	Expect(Env.K8s.Get(ctx, client.ObjectKey{Namespace: AutoscalerNamespace, Name: AutoscalerDeployment}, &deployment)).To(Succeed())

	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == AutoscalerContainer {
			return container
		}
	}

	Fail("autoscaler container is missing")

	return corev1.Container{}
}

// PriorityClasses checks the cutoff flag and returns the run's priority names.
func PriorityClasses(ctx context.Context) (low, high string) {
	container := ControllerContainer(ctx)
	Expect(slices.Concat(container.Command, container.Args)).To(ContainElement("--expendable-pods-priority-cutoff=-10"))
	low, high = Env.Config.RunID+"-expendable", Env.Config.RunID+"-high"

	return low, high
}

// PriorityDeployment returns a memory Deployment that uses PriorityClass class.
func PriorityDeployment(name, pool, class string, replicas int32, bytes int64) *appsv1.Deployment {
	workload := MemoryDeployment(name, pool, replicas, bytes)
	workload.Spec.Template.Spec.PriorityClassName = class

	return workload
}

// ObserveKept checks that workers and their workload remain for five minutes.
func ObserveKept(ctx context.Context, grown Snapshot, namespace, name string, replicas int) {
	Consistently(ctx, func(g Gomega) {
		current, err := ReadActiveSnapshot(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(current.Ready(Env.Config, 2, 1)).To(Succeed())
		g.Expect(current.Pools).To(BeComparableTo(grown.Pools,
			cmpopts.IgnoreFields(PoolState{}, "ProvisioningState"),
			cmpopts.IgnoreFields(Instance{}, "ProvisioningState")))
		_, err = Env.WorkloadState(ctx, namespace, name, "", replicas, 0)
		g.Expect(err).NotTo(HaveOccurred())
	}, 5*time.Minute, PollInterval).Should(Succeed())
}
