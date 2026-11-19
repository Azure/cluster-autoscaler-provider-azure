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

package general

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/Azure/cluster-autoscaler-provider-azure/test/e2e/utils"
)

var _ = Describe("Azure Provider", func() {
	BeforeEach(utils.SetupSpec, NodeTimeout(20*time.Minute))
	It(
		"drains kube-system Pods that a PodDisruptionBudget protects",
		Label("system-pods", "Feature:ClusterSizeAutoscalingScaleDown", utils.TestSuiteLabelSlow),
		func(ctx SpecContext) {
			utils.Drain(ctx, 2, 1, "kube-system")
		},
		NodeTimeout(55*time.Minute),
	)
})
