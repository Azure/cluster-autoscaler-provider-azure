# Azure E2E

The suite runs on disposable AKS with this repository's autoscaler, not AKS's managed autoscaler. One kind management cluster creates four workload clusters concurrently from the same CAPZ/ASO template. One Ginkgo invocation distributes the 3 specs across four processes. Each process's `BeforeSuite` prepares only its bound cluster.

| First-set coverage | Source | Preparation |
| --- | --- | --- |
| Tagged discovery, five-minute idle, maximum and physical deletion, scale from zero | `e2e/basic/` | Base |

Main uses min 1/max 2 and zero min 0/max 1. User workers have 110 Pod slots and isolation taints; only test workloads and DaemonSets occupy them. D2s_v5 uses 1000m demand Pods, checked so one fits and two do not. The active-node cap is four.

## Run

Dispatch **Azure E2E** on trusted code. It accepts Kubernetes version (default `1.37.0`), region and `cluster_count` (default 4, range 1 to 16). It has no push, PR or scheduled triggers. GitHub requires the dispatch workflow on the default branch before it can run against another branch.

| Repository variable | Value |
| --- | --- |
| `E2E_RUNS_ON` | JSON 1ES label array, such as `["self-hosted","linux","x64","your-pool"]` |
| `E2E_CLIENT_ID` | Runner user-assigned managed identity client ID |
| `E2E_SUBSCRIPTION_ID` | Disposable subscription ID |
| `E2E_CONTROL_PLANE_IDENTITY_ID` | Pre-provisioned control-plane UAMI resource ID |
| `E2E_KUBELET_IDENTITY_ID` | Pre-provisioned autoscaler/kubelet UAMI resource ID |
| `E2E_ACR_NAME` | Pre-provisioned shared ACR name |
| `E2E_LOCATION` | Region with AKS version and VM quota |
| `E2E_RESOURCE_PREFIX` | Optional lowercase alphanumeric prefix starting with `e2ea`, up to 19 characters |

An admin provisions these resources and roles once, outside the workflow. Both UAMIs and the ACR must be in the E2E subscription, so the runner's subscription Contributor role covers attaching the identities.

| One-time resource | Required roles |
| --- | --- |
| Runner UAMI (`E2E_CLIENT_ID`) | Contributor on the subscription; AcrPush on the shared ACR |
| Autoscaler/kubelet UAMI | Contributor on the subscription; AcrPull on the shared ACR |
| Control-plane UAMI | Managed Identity Operator on the autoscaler/kubelet UAMI |
| Shared ACR | Existing registry with registry-level RBAC permissions |

The workflow creates no identities or role assignments and needs no RBAC Administrator role. AKS grants its control-plane identity Contributor on the managed node resource group, as it does for every cluster; that service-managed grant disappears with the group.

The Ubuntu runner needs Docker/buildx, Git, Make, jq and Azure CLI, at least 16 GB RAM and 40 GB free disk. Actions installs the latest Go 1.26 patch because the CAPZ checkout follows its matching release branch; CAPZ installs its tools. Allow up to eight workload vCPUs per cluster with D2s_v5, plus the runner.

Before creating a cluster, read-only ARM calls confirm that both UAMIs and the shared ACR exist. The controller uses the supplied kubelet identity through IMDS; its client ID is checked against ManagedCluster status. No client secret or federation is needed. ASO's `serviceoperator.azure.com/reconcile-policy=skip` annotation freezes Azure reconciliation after preparation. The CAPI pause is removed before cleanup deletes the Cluster.

Shell retains CAPZ creation, image build, bindings, collection and cleanup, including after a crashed test process. Go uses resource-named helpers in `e2e/utils`, following cloud-provider-azure's E2E layout. Preparation checks the fixture, waits for settlement, pins metrics-server, drains add-ons to pool0, freezes CAPI/ASO and installs the controller through the Helm CLI. Azure clients use the binding's scope; node drains use CAPZ's `kubectl`.

Each run records exactly `2 * cluster_count` groups and its run-unique image tags. Cleanup deletes and verifies the groups and untags exactly those images, never the shared ACR or other tags. It still cleans up after build or test failure. Cluster one keeps `CLUSTER_NAME`; later clusters append `c2`, `c3`, etc. Reports record each spec's process, run ID and resource group. Raw CAPZ dumps, kubeconfigs and credentials are not uploaded. Runs serialize per subscription with a 470-minute job deadline, including cleanup. Operators must provide external cleanup if a runner dies or a workflow is cancelled.

For local preparation on a disposable Azure VM, run `az login --identity --client-id <runner-client-id>`. Export `AZURE_CLIENT_ID`, `AZURE_SUBSCRIPTION_ID`, `AZURE_LOCATION`, `AZURE_CONTROL_PLANE_IDENTITY_ID`, `AZURE_KUBELET_IDENTITY_ID` and `E2E_ACR_NAME`, plus `KUBERNETES_VERSION`, a unique lowercase alphanumeric `CLUSTER_NAME` starting with `e2ea`, and absolute `E2E_ROOT`. Run `bash test/hack/ci-e2e.sh run`. `E2E_CLUSTER_COUNT` defaults to 4. Keep the same count for `run`, `collect` and `cleanup`. Always run `collect` and `cleanup`, including after failures. Reports are saved under `$E2E_ROOT/artifacts`; direct Ginkgo runs use `ARTIFACTS` (default `_artifacts`).

Run `make -C test e2etests ENVIRONMENT=/absolute/path/to/environment.json LABEL_FILTER='<labels>'` against a freshly created disposable fixture. The JSON contains `kubeconfig`, `managementKubeconfig`, `clusterName`, `resourceGroup` (the node resource group), `subscriptionID`, `kubeletClientID`, `expectedImage`. The cluster name is also its resource group and run ID. Go discovers VMSS names from `aks-managed-poolName` tags. Controller names, Node labels, the pinned workload image and 1000m CPU request are constants. Each report directory must be unique; no default cluster is selected.

Direct runs default to one Ginkgo process and a single JSON object. For parallel runs, set `E2E_CLUSTER_COUNT=N` and supply an ordered JSON array of N bindings. Process i uses array entry i; workload kubeconfigs, node resource groups and cluster names must be distinct. The management kubeconfig is shared. Label filters select specs.

## Local checks

`test/hack/capz.sh` defines `CAPZ_VERSION` once and derives the matching `release-<major>.<minor>` runtime branch, following upstream autoscaler Prow. Runs record the resolved CAPZ commit in `capz-commit.txt` alongside `source-commit.txt`. Template generation fetches the tag into `test/.tools/capz-$CAPZ_VERSION`, copies our relative overlay into `templates/test/autoscaler-e2e/`, and builds with CAPZ's own kustomize target. GitHub access is required; the existing verify workflow runs `make -C test verify-templates`.

Bump `CAPZ_VERSION` in `test/hack/capz.sh` and run `make -C test generate-templates`. To try another tag without changing the default, use `CAPZ_VERSION=vX.Y.Z make -C test verify-templates`. Patch-only changes also need regeneration.

```sh
make -C test test-local
```
