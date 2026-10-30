REPO_ROOT:=$(shell git rev-parse --show-toplevel)
CAS_ROOT:=$(REPO_ROOT)
DEV_DIR:=$(CAS_ROOT)/deploy/dev
REGISTRY?=localhost
IMAGE?=$(REGISTRY)/cluster-autoscaler
TAG?=dev
GOARCH?=$(shell go env GOARCH)

TEST_TIMEOUT?=6h
ENVIRONMENT?=
E2E_CLUSTER_COUNT?=1

FOCUS?=
LABEL_FILTER?=
ARTIFACTS?=_artifacts

# --------------------------------------------------------------------------
# CI targets (build image and run against a CAPZ AKS cluster)
# --------------------------------------------------------------------------

.PHONY: build-e2e
build-e2e:
	$(MAKE) -C $(CAS_ROOT) make-image-arch-$(GOARCH) IMAGE=$(IMAGE) TAG=$(TAG)
	docker push $(IMAGE)-$(GOARCH):$(TAG)

.PHONY: test-e2e
test-e2e: build-e2e ## Build, push, bind every prepared cluster and run the suite
	IMAGE="$(IMAGE)-$(GOARCH)" TAG="$(TAG)" ARTIFACTS="$(ARTIFACTS)" \
		LABEL_FILTER="$(LABEL_FILTER)" \
		TEST_TIMEOUT="$(TEST_TIMEOUT)" bash hack/ci-e2e.sh test

# --------------------------------------------------------------------------
# Local development helpers
# --------------------------------------------------------------------------
# These helpers do not create the main/zero fixture for the E2E suite.
# See README.md for CAPZ setup or pass ENVIRONMENT=<JSON binding> to e2etests.

.PHONY: help
help: ## Display help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Cluster Setup (one-time)

.PHONY: setup-cluster
setup-cluster: ## Create AKS + ACR + workload identity for development
	cd $(DEV_DIR) && bash ./aks-dev-deploy.sh

##@ Build & Deploy

.PHONY: deploy-local
deploy-local: ## Build CAS and deploy to cluster via skaffold
	cd $(CAS_ROOT) && skaffold run --filename deploy/dev/skaffold.yaml

.PHONY: deploy-local-dev
deploy-local-dev: ## Build + deploy CAS in watch mode (auto-redeploy on changes)
	cd $(CAS_ROOT) && skaffold dev --filename deploy/dev/skaffold.yaml

##@ E2E Testing

.PHONY: e2etests
e2etests: ## Run E2Es with an explicit environment JSON binding
	@test -n "$(ENVIRONMENT)" || (echo "ENVIRONMENT is required"; exit 1)
	go run -mod=readonly github.com/onsi/ginkgo/v2/ginkgo \
		--tags e2e \
		-v --trace --fail-on-empty \
		--procs="$(E2E_CLUSTER_COUNT)" \
		--timeout $(TEST_TIMEOUT) \
		--output-dir "$(ARTIFACTS)" \
		--junit-report="junit.e2e_suite.1.xml" \
		--json-report="report.e2e_suite.1.json" \
		$(if $(FOCUS),--focus="$(FOCUS)",) \
		$(if $(LABEL_FILTER),--label-filter="$(LABEL_FILTER)",) \
		./e2e -- \
		-environment="$(ENVIRONMENT)"

.PHONY: test-local
test-local: ## Check the library and spec registration without Azure
	go test -mod=readonly -race -tags=e2e ./e2e/... -skip '^TestE2E$$'
	go test -mod=readonly -tags=e2e ./e2e -run '^TestE2E$$' -count=1 -args -ginkgo.dry-run
