# Testing

## Local checks

Use Go 1.26 or later, Git, Make and a C toolchain for race tests. Run commands from the repository root. Local tests do not need Azure credentials or a Kubernetes cluster.

| Command | Checks |
| --- | --- |
| `make test-azure` | Azure provider tests with the race detector |
| `make test-unit` | Application build and root module tests with race detection and vet |
| `make test-ci` | Application build and root module tests |

The build creates a Linux binary by default. Tests still run on the host.

For focused tests:

```sh
go test ./pkg/cloudprovider/azure -run TestBuildAzureConfigPrecedence -count=1
```

For boilerplate and spelling:

```sh
hack/verify-boilerplate.sh
hack/verify-spelling.sh
```

Spelling checks need `misspell` on PATH. If it is missing, install the tool used by [CI](../hack/install-verify-tools.sh):

```sh
go install github.com/golangci/misspell/cmd/misspell@latest
export PATH="$(go env GOPATH)/bin:$PATH"
```

## Chart checks

With Helm and chart-testing available:

```sh
helm lint --strict charts/cluster-autoscaler
ct lint --chart-dirs charts --target-branch main --validate-maintainers=false
```

The [chart workflow](../.github/workflows/pr.yaml) runs chart-testing lint and kind installation for chart changes. It also checks that the README matches helm-docs output. See the [chart development guide](../charts/README.md#generate-the-readme) for generation. Local Helm lint does not check version increments or install the chart.

## Patched-core resume check

The released core does not count an old, unready resumed Node as starting. [Deallocate mode](suspended-nodes.md) requires the proposed resume patch in `core-changes-combined` at `7e24d48f`, which is not pinned in this repository. Use a local core checkout that contains that commit. No Azure resources are needed.

From the provider repository root, create a temporary copy of the patched core and an external workspace:

```sh
PROVIDER_REPO=$(pwd)
CORE_REPO=/path/to/local/cluster-autoscaler
CHECK_DIR=$(mktemp -d)
mkdir "$CHECK_DIR/core"
git -C "$CORE_REPO" archive 7e24d48f0731058aeef825331884897c56c2c17c | tar -x -C "$CHECK_DIR/core"
(cd "$CHECK_DIR" && go work init "$PROVIDER_REPO" "$CHECK_DIR/core")
GOWORK="$CHECK_DIR/go.work" go test -race sigs.k8s.io/cluster-autoscaler/pkg/clusterstate \
  -run 'Test(ResumedNodeReadiness|UpcomingNodesAfterResume)$' -count=1
GOWORK="$CHECK_DIR/go.work" make test-ci
```

The focused command checks the patch's existing readiness and upcoming-capacity regressions. It does not run Azure end-to-end validation. The provider tests separately cover park/resume and running min/max limits against fake Azure responses.

Run `GOWORK=off make test-ci` to check the unchanged released dependency. Run the chart checks above separately. Do not commit the temporary workspace or add a core replacement to `go.mod`. Remove the specific CHECK_DIR when finished.

## CI

[ca-test.yaml](../.github/workflows/ca-test.yaml) runs `make test-ci`. [verify.yaml](../.github/workflows/verify.yaml) runs formatting, boilerplate, lint and spelling checks.
