//go:build e2e

/*
Copyright The Kubernetes Authors.

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

package lifecycle

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

type diskPodState struct {
	PodUID       types.UID
	Node         string
	ClaimUID     types.UID
	VolumeUID    types.UID
	VolumeName   string
	VolumeHandle string
	Identity     string
}

var _ = Describe("Azure Provider", func() {
	BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
	It(
		"moves StatefulSet Azure Disks to the remaining node after deleting a node",
		Label("disk", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			baseline := utils.WaitStable(ctx, 1, 0)
			Expect(utils.Env.CheckDiskFixture(ctx, baseline)).To(Succeed())
			Expect(utils.Env.DemandFits(ctx, baseline, utils.Env.Config.MainPool)).To(Succeed())

			By("growing main to two workers")
			growth := utils.Env.Deployment(utils.Namespace.Name, "disk-growth", utils.MainLabel, utils.DemandCPU, 2)
			Expect(utils.Env.K8s.Create(ctx, growth)).To(Succeed())
			grown := utils.WaitStable(ctx, 2, 0)
			utils.AssertDistinctNodes(utils.WaitWorkload(ctx, growth.Name, utils.Env.Config.MainPool, 2, 0), 2)

			Eventually(
				ctx,
				func() error {
					return utils.Env.CheckDiskWorkers(ctx, grown)
				},
				3*time.Minute,
				utils.PollInterval,
			).Should(Succeed())

			By("starting a StatefulSet with one Azure Disk on each worker")
			stateful := diskStatefulSet("disk-data")
			Expect(utils.Env.K8s.Create(ctx, stateful)).To(Succeed())

			var before map[string]diskPodState

			Eventually(
				ctx,
				func() error {
					var err error
					before, err = diskPods(ctx, stateful)
					if err != nil {
						return err
					}

					placed := map[string]bool{}

					for name, pod := range before {
						placed[pod.Node] = true
						if pod.Identity != string(pod.PodUID) {
							return fmt.Errorf("StatefulSet Pod %s wrote %q to its disk, want its UID %s", name, pod.Identity, pod.PodUID)
						}
					}

					if len(placed) != 2 {
						return fmt.Errorf("StatefulSet Pods must start on separate main workers, got %d workers", len(placed))
					}

					return nil
				},
				utils.SettleTimeout,
				utils.PollInterval,
			).Should(Succeed())

			By("removing the growth demand and waiting for one worker to be deleted")
			Expect(utils.Env.K8s.Delete(ctx, growth)).To(Succeed())
			utils.WaitDeleted(ctx, grown, utils.Env.Config.MainPool, 1, 1, 0)

			By("checking that the disks and their data moved to the surviving worker")
			var after map[string]diskPodState
			deleted := utils.WaitStable(ctx, 1, 0)
			survivor := utils.PoolNodes(deleted.Nodes, utils.Env.Config.PoolID(utils.Env.Config.MainPool))[0].Name

			Eventually(
				ctx,
				func() error {
					var err error
					after, err = diskPods(ctx, stateful)
					if err != nil {
						return err
					}

					for name, original := range before {
						current, ok := after[name]
						if !ok || current.Node != survivor || current.ClaimUID != original.ClaimUID ||
							current.VolumeUID != original.VolumeUID || current.VolumeName != original.VolumeName ||
							current.VolumeHandle != original.VolumeHandle || current.Identity != original.Identity {
							return fmt.Errorf("StatefulSet Pod %s did not keep its attached disk and data on the surviving worker", name)
						}

					}

					return nil
				},
				utils.SettleTimeout,
				utils.PollInterval,
			).Should(Succeed())

			AddReportEntry("azure-disks", fmt.Sprintf("%d PVCs kept their PVs, Azure disk handles and file contents; at least one disk reattached after physical deletion", len(after)))
		},
		NodeTimeout(65*time.Minute),
	)
})

// diskStatefulSet returns a two-replica StatefulSet on main with one Azure
// Disk claim per Pod. Each Pod writes its UID to the disk on first start and
// is Ready only when the file is present.
func diskStatefulSet(name string) *appsv1.StatefulSet {
	workload := utils.Env.Deployment(utils.Namespace.Name, name, utils.MainLabel, "50m", 2)
	template := workload.Spec.Template
	template.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"}
	template.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
			Weight: 100, PodAffinityTerm: corev1.PodAffinityTerm{
				TopologyKey: corev1.LabelHostname, LabelSelector: workload.Spec.Selector.DeepCopy(),
			},
		}},
	}}
	container := &template.Spec.Containers[0]
	container.Command = []string{
		"sh",
		"-c",
		`if [ ! -f /data/identity ]; then printf '%s' "$POD_UID" > /data/identity; fi; ` + utils.InfiniteSleepCommand,
	}
	container.Env = []corev1.EnvVar{{Name: "POD_UID", ValueFrom: &corev1.EnvVarSource{
		FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"},
	}}}
	container.VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}
	container.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
		Exec: &corev1.ExecAction{Command: []string{
			"sh",
			"-c",
			"test -s /data/identity",
		}},
	}, PeriodSeconds: 5}

	return &appsv1.StatefulSet{
		ObjectMeta: workload.ObjectMeta,
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         name,
			Replicas:            ptr.To(int32(2)),
			Selector:            workload.Spec.Selector,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Template:            template,
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: workload.Labels},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: ptr.To(utils.Env.Config.RunID + "-disk"),
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					}},
				},
			}},
		},
	}
}

// diskPods returns the Pod, claim, volume and disk file of each Ready
// StatefulSet Pod. It returns an error unless both Pods run on main, each
// claim is bound to an Azure Disk in the selected resource group, and each
// disk is attached to its Pod's worker.
func diskPods(ctx context.Context, stateful *appsv1.StatefulSet) (map[string]diskPodState, error) {
	var current appsv1.StatefulSet

	if err := utils.Env.K8s.Get(ctx, client.ObjectKeyFromObject(stateful), &current); err != nil {
		return nil, fmt.Errorf("get StatefulSet: %w", err)
	}

	if current.Status.ObservedGeneration < current.Generation || current.Status.ReadyReplicas != 2 {
		return nil, fmt.Errorf("StatefulSet has %d Ready replicas, want two", current.Status.ReadyReplicas)
	}

	pods, err := utils.Env.WorkloadState(ctx, stateful.Namespace, stateful.Name, utils.Env.Config.MainPool, 2, 0)
	if err != nil {
		return nil, err
	}

	var attachments storagev1.VolumeAttachmentList

	if err := utils.Env.K8s.List(ctx, &attachments); err != nil {
		return nil, fmt.Errorf("list VolumeAttachments: %w", err)
	}

	result := map[string]diskPodState{}
	diskPrefix := strings.ToLower("/subscriptions/" + utils.Env.Config.SubscriptionID + "/resourceGroups/" +
		utils.Env.Config.ResourceGroup + "/providers/Microsoft.Compute/disks/")

	for _, pod := range pods {
		claimName := "data-" + pod.Name
		var claim corev1.PersistentVolumeClaim

		if err := utils.Env.K8s.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: claimName}, &claim); err != nil {
			return nil, fmt.Errorf("get claim %s: %w", claimName, err)
		}

		if claim.UID == "" || claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName == "" ||
			claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != utils.Env.Config.RunID+"-disk" {
			return nil, fmt.Errorf("StatefulSet claim %s is not bound through the disk class", claimName)
		}

		var volume corev1.PersistentVolume

		if err := utils.Env.K8s.Get(ctx, client.ObjectKey{Name: claim.Spec.VolumeName}, &volume); err != nil {
			return nil, fmt.Errorf("get volume %s: %w", claim.Spec.VolumeName, err)
		}

		if volume.UID == "" || volume.Spec.ClaimRef == nil || volume.Spec.ClaimRef.UID != claim.UID ||
			volume.Spec.CSI == nil || volume.Spec.CSI.Driver != utils.DiskCSIDriver ||
			!strings.HasPrefix(strings.ToLower(volume.Spec.CSI.VolumeHandle), diskPrefix) {
			return nil, fmt.Errorf("StatefulSet claim %s does not own an Azure Disk in the selected resource group", claimName)
		}

		attached := false

		for _, attachment := range attachments.Items {
			if attachment.Spec.Source.PersistentVolumeName != nil &&
				*attachment.Spec.Source.PersistentVolumeName == volume.Name &&
				attachment.Spec.NodeName == pod.Spec.NodeName &&
				attachment.Spec.Attacher == utils.DiskCSIDriver && attachment.Status.Attached {
				attached = true
			}
		}

		if !attached {
			return nil, fmt.Errorf("Azure Disk %s is not attached to the Ready Pod worker", volume.Name)
		}

		identity, err := utils.Env.ReadDiskIdentity(ctx, pod.Namespace, pod.Name)
		if err != nil {
			return nil, err
		}

		if identity == "" {
			return nil, fmt.Errorf("StatefulSet Pod %s has an empty disk file", pod.Name)
		}

		result[pod.Name] = diskPodState{
			PodUID:       pod.UID,
			Node:         pod.Spec.NodeName,
			ClaimUID:     claim.UID,
			VolumeUID:    volume.UID,
			VolumeName:   volume.Name,
			VolumeHandle: volume.Spec.CSI.VolumeHandle,
			Identity:     identity,
		}
	}

	return result, nil
}
