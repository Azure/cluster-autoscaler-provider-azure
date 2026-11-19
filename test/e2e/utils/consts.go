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

// RunLabel identifies workloads owned by this run.
const RunLabel = "autoscaler-e2e-run"

const (
	// AutoscalerNamespace is the chart's installation namespace.
	AutoscalerNamespace = "default"
	// AutoscalerDeployment is the chart's Deployment name.
	AutoscalerDeployment = "cluster-autoscaler-azure-cluster-autoscaler"
	// AutoscalerContainer is the controller's container name.
	AutoscalerContainer = "azure-cluster-autoscaler"
	// PoolLabel selects the test worker pools.
	PoolLabel = "acceptance-pool"
	// MainLabel selects main workers.
	MainLabel = "main"
	// ZeroLabel selects zero-pool workers.
	ZeroLabel = "zero"
	// BalanceLabel selects both similar test pools.
	BalanceLabel = "balance"
	// DemandCPU fits once, but not twice, on each test worker.
	DemandCPU = "1000m"
	// WorkloadImage pins the sleeping workload container.
	WorkloadImage = "busybox@sha256:b7f3d86d6e84fc17718c48bcde1450807faa2d56704205c697b4bd5df7b9e29f"
)

const (
	// TestSuiteLabelSmoke selects the provider smoke specs.
	TestSuiteLabelSmoke = "smoke"
	// TestSuiteLabelSlow selects long-running specs.
	TestSuiteLabelSlow = "Slow"
	// TestSuiteLabelTaint selects the tainted scale-from-zero phase.
	TestSuiteLabelTaint = "taint"
	// TestSuiteLabelMinimum selects minimum enforcement.
	TestSuiteLabelMinimum = "minimum"
	// TestSuiteLabelBalance selects the balancing phase.
	TestSuiteLabelBalance = "balance"
	// TestSuiteLabelNoJoin selects the never-registers phase.
	TestSuiteLabelNoJoin = "no-join"
	// TestSuiteLabelDeallocate selects the optional deallocate phases.
	TestSuiteLabelDeallocate = "deallocate"
)
