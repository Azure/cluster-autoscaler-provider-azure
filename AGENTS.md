# AGENTS.md

Guidance for coding agents working in this repository. Humans should start with the [README](README.md) and [contributing guide](CONTRIBUTING.md).

## Project

This repository is the Azure cloud provider for [Cluster Autoscaler](https://github.com/kubernetes-sigs/cluster-autoscaler), packaged as a standalone binary and Helm chart. It is pre-release: there is no official image, and the chart's default image is a placeholder.

The autoscaling core (scheduling simulation, scale-up and scale-down loop) is the external module `sigs.k8s.io/cluster-autoscaler`, pinned in [go.mod](go.mod). This repository holds only the Azure provider, its registration, the entry point, packaging and tests. Changes to core behavior belong in kubernetes-sigs/cluster-autoscaler, not here.

| Path | Contents |
| --- | --- |
| [main.go](main.go) | Entry point; builds clients and calls the core's autoscaler builder |
| [pkg/cloudprovider/azure](pkg/cloudprovider/azure) | The provider: configuration, discovery, caches, node groups, node templates, Azure clients |
| [pkg/cloudprovider/router](pkg/cloudprovider/router) | Registers only Azure |
| [pkg/version](pkg/version) | Version set through linker flags |
| [charts/cluster-autoscaler](charts/cluster-autoscaler) | Helm chart; its README is generated |
| [deploy](deploy) | Example manifests |
| [test](test) | Separate Go module for live E2E tests |
| [hack](hack) and [docs](docs) | Verify scripts and guides |

Read [docs/architecture.md](docs/architecture.md) before changing discovery, node templates, scaling or module boundaries.

## Setup

- Go 1.26 or later, Git, Make, and a C toolchain for race tests. Helm and chart-testing (`ct`) for chart work, Docker for images.
- Local tests need no Azure credentials and no cluster.
- Lint tools: `hack/install-verify-tools.sh` installs golint and misspell into `$(go env GOPATH)/bin`; put that on `PATH`.

## Build and test

Run from the repository root.

- `make build`: Linux binary `cluster-autoscaler-<arch>`. Use `make build GOOS=darwin` on macOS.
- `make test-azure`: provider tests with race detection and vet. Use this for the inner loop.
- `make test-ci`: build plus all root-module tests with race detection and vet. CI runs this.
- One test: `go test ./pkg/cloudprovider/azure -run TestBuildAzureConfigPrecedence -count=1`
- `make make-image TAG=dev`: builds `localhost/cluster-autoscaler-<arch>:dev` locally; it doesn't publish.

A root `go test ./...` does not include `test/`, which is a separate module with its own `go.mod` and Makefile.

## Before opening a PR

```sh
make format
hack/verify-all.sh -v
make test-ci
git diff --check
```

`hack/verify-all.sh` runs the gofmt, golint, boilerplate and spelling checks that the `verify` CI job runs. `hack/verify-golint.sh` needs bash 4 or later; on macOS put Homebrew bash first on `PATH`, because `verify-all.sh` calls `bash` from `PATH`.

For chart changes:

- Bump `version` in [Chart.yaml](charts/cluster-autoscaler/Chart.yaml); CI fails a changed chart without a version increment. Leave `appVersion` alone unless asked.
- Run `helm lint --strict charts/cluster-autoscaler` and `ct lint --chart-dirs charts --target-branch main --validate-maintainers=false`.
- If `values.yaml` or `README.md.gotmpl` changed, regenerate the chart README from the repository root with `go run github.com/norwoodj/helm-docs/cmd/helm-docs@v1.14.2` and commit it. Don't use the older helm-docs pinned in `.pre-commit-config.yaml`.

## Generated code

- The `azure_mock_*.go` files are mockgen output (`go.uber.org/mock`). The commands are the `//go:generate` lines in `azure_client.go` and `azure_vmss_delete_client.go`. Regenerate instead of editing by hand.
- `azure_instance_types.go` comes from `azure_instance_types/gen.go`, which runs `az vm list-skus` and needs an Azure login. `go generate ./pkg/cloudprovider/azure` runs it too, so run the mockgen commands on their own unless you mean to refresh instance types.

## Code style

- Idiomatic Go that matches nearby code and the [Go review comments](https://go.dev/wiki/CodeReviewComments). `make format` applies `gofmt -s`, and golint must pass.
- Keep comments short and only where they explain something non-obvious.
- Put tests next to the code as `*_test.go`, prefer table-driven tests, and use the existing fakes (`azure_fakes.go`) and mocks instead of real Azure.
- Every Go, shell and Python file needs the Apache 2.0 header that `hack/verify-boilerplate.sh` checks. Files that came from kubernetes/autoscaler keep `Copyright The Kubernetes Authors.`, even after changes. New files use the same header with `Copyright (c) Microsoft Corporation.` and no year.
- Keep dependency pins unless the change needs an update. Kubernetes libraries are pinned through `replace` directives and move together with the core version; Dependabot ignores them on purpose.
- Some tests use `kubemark://` and `kwok://` provider IDs on purpose, as negative cases.

## Docs

- Update the README, `docs/`, the provider README and the chart README when behavior, flags or values change.
- Link to code or existing docs instead of repeating them.
- Put each Markdown paragraph and list item on one line; don't hard-wrap. Leave text copied verbatim, such as the CLA and trademark notices, as it is.

## Pull requests

- PRs are squash-merged, and the title becomes the commit subject. Use Conventional Commits, for example `feat(azure): ...`, `fix(azure): ...`, `feat(chart): ...`, `test(e2e): ...`, `docs: ...`, `ci: ...`, `chore: ...` or `build(deps): ...`.
- Fill in the [PR template](.github/PULL_REQUEST_TEMPLATE.md), including the `release-note` block (`NONE` when nothing is user-facing). List the checks you ran and what you didn't test.
- Merging needs the `test` and `verify` checks and one approving review. The Microsoft CLA bot comments on first-time contributors.

## Safety

- Never commit credentials, kubeconfigs, subscription IDs or test artifacts.
- Don't run anything that creates or changes Azure resources, including live E2E runs, unless the user asks. Live runs cost money and need a disposable subscription and a cleanup plan.
- Don't run a second autoscaler against pools that another autoscaler manages.
- Report vulnerabilities through [SECURITY.md](SECURITY.md), not public issues.
