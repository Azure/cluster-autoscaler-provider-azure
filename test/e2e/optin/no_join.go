//go:build e2e

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

package optin

import (
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

// RegisterNoJoin adds the never-registers timeout and deletion phase.
func RegisterNoJoin() {
	Describe("no-join phase", func() {
		BeforeAll(func(ctx SpecContext) {
			Expect(utils.Env.PreparePhase(ctx, utils.TestSuiteLabelNoJoin)).To(Succeed())
		}, NodeTimeout(75*time.Minute))
		BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
		It(
			"deletes an AKS VM that never registers",
			Label(utils.TestSuiteLabelNoJoin, utils.TestSuiteLabelSlow),
			func(ctx SpecContext) {
				By("denying zero-pool Node registration")
				c := utils.Env.Config
				container := utils.ControllerContainer(ctx)
				Expect(slices.Concat(container.Command, container.Args)).To(ContainElement("--max-node-provision-time=5m"))
				policy := &admissionv1.ValidatingAdmissionPolicy{
					ObjectMeta: metav1.ObjectMeta{Name: c.RunID + "-deny-zero", Labels: map[string]string{utils.RunLabel: c.RunID}},
					Spec: admissionv1.ValidatingAdmissionPolicySpec{
						FailurePolicy: ptr.To(admissionv1.Fail),
						MatchConstraints: &admissionv1.MatchResources{ResourceRules: []admissionv1.NamedRuleWithOperations{{
							RuleWithOperations: admissionv1.RuleWithOperations{Operations: []admissionv1.OperationType{admissionv1.Create},
								Rule: admissionv1.Rule{
									APIGroups:   []string{""},
									APIVersions: []string{"v1"},
									Resources:   []string{"nodes"},
								}},
						}}},
						Validations: []admissionv1.Validation{{Expression: `!has(object.metadata.labels) || !('kubernetes.azure.com/agentpool' in object.metadata.labels) || object.metadata.labels['kubernetes.azure.com/agentpool'] != 'zero'`,
							Message: "E2E blocks zero-pool Node registration"}},
					},
				}
				_, err := utils.Env.Kube.AdmissionregistrationV1().ValidatingAdmissionPolicies().Create(ctx, policy, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())

				DeferCleanup(func(ctx SpecContext) {
					Expect(client.IgnoreNotFound(utils.Env.Kube.AdmissionregistrationV1().ValidatingAdmissionPolicies().Delete(
						ctx, policy.Name, metav1.DeleteOptions{}))).To(Succeed())
				})
				binding := &admissionv1.ValidatingAdmissionPolicyBinding{
					ObjectMeta: metav1.ObjectMeta{Name: policy.Name},
					Spec:       admissionv1.ValidatingAdmissionPolicyBindingSpec{PolicyName: policy.Name, ValidationActions: []admissionv1.ValidationAction{admissionv1.Deny}},
				}
				_, err = utils.Env.Kube.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Create(ctx, binding, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())

				DeferCleanup(func(ctx SpecContext) {
					Expect(client.IgnoreNotFound(utils.Env.Kube.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Delete(
						ctx, binding.Name, metav1.DeleteOptions{}))).To(Succeed())
				})

				Eventually(
					ctx,
					func() error {
						probe := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: c.RunID + "-probe", Labels: map[string]string{"kubernetes.azure.com/agentpool": "zero"}}}
						_, err := utils.Env.Kube.CoreV1().Nodes().Create(ctx, probe, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
						if !apierrors.IsInvalid(err) && !apierrors.IsForbidden(err) {
							return fmt.Errorf("zero Node probe was not rejected by admission: %v", err)
						}

						if !strings.Contains(err.Error(), "E2E blocks zero-pool") {
							return fmt.Errorf("zero probe failed for an unrelated reason: %w", err)
						}

						probe.Labels["kubernetes.azure.com/agentpool"] = "main"
						_, err = utils.Env.Kube.CoreV1().Nodes().Create(ctx, probe, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})

						return err
					},
					time.Minute,
					utils.PollInterval,
				).Should(Succeed())
				By("creating demand for an unregistered VM")
				work := utils.Env.Deployment(utils.Namespace.Name, "unregistered", utils.ZeroLabel, "50m", 1)
				started := time.Now()
				Expect(utils.Env.K8s.Create(ctx, work)).To(Succeed())

				DeferCleanup(func(ctx SpecContext) {
					Expect(client.IgnoreNotFound(utils.Env.K8s.Delete(ctx, work))).To(Succeed())

					Eventually(
						ctx,
						func() ([]corev1.Pod, error) { return utils.ListWorkload(ctx, work.Name) },
						3*time.Minute,
						utils.PollInterval,
					).Should(BeEmpty())
				}, NodeTimeout(4*time.Minute))
				var created utils.Snapshot

				Eventually(
					ctx,
					func() error {
						var err error
						created, err = utils.ReadActiveSnapshot(ctx)
						if err != nil {
							return err
						}

						if len(utils.PoolNodes(created.Nodes, c.PoolID(c.ZeroPool))) != 0 {
							StopTrying("denied VM registered as a Node").Now()
						}

						pool := created.Pools[c.ZeroPool]
						if pool.Capacity != 1 || len(pool.Instances) != 1 {
							return fmt.Errorf("waiting for one provisioned unregistered VM")
						}

						for _, vm := range pool.Instances {
							if vm.ProvisioningState != "Succeeded" || len(vm.NICs) == 0 {
								return fmt.Errorf("unregistered VM has not provisioned successfully")
							}
						}

						return nil
					},
					utils.SettleTimeout,
					utils.PollInterval,
				).Should(Succeed())

				AddReportEntry("unregistered-vm", created.Pools[c.ZeroPool])

				By("waiting for timeout and physical deletion")
				Eventually(
					ctx,
					func() error {
						after, err := utils.ReadActiveSnapshot(ctx)
						if err != nil {
							return err
						}

						if len(utils.PoolNodes(after.Nodes, c.PoolID(c.ZeroPool))) != 0 {
							StopTrying("denied VM registered as a Node").Now()
						}

						var events corev1.EventList

						if err := utils.Env.K8s.List(ctx, &events, client.InNamespace(utils.AutoscalerNamespace)); err != nil {
							return err
						}

						if !scaleUpTimedOutSince(events.Items, c.ZeroPool, started) {
							return fmt.Errorf("waiting for ScaleUpTimedOut")
						}

						if err := after.Stable(c, 1, 0); err != nil {
							return err
						}

						return utils.Env.Deleted(ctx, created, after, c.ZeroPool, 1)
					},
					utils.SettleTimeout,
					utils.PollInterval,
				).Should(Succeed())

				By("checking that backoff prevents immediate regrowth")
				utils.ObserveNoGrowth(ctx, work.Name, c.ZeroPool, 0, 1, time.Minute)
			},
			NodeTimeout(40*time.Minute),
		)
	})
}
