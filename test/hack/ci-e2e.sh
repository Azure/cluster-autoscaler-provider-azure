#!/usr/bin/env bash

# Copyright (c) Microsoft Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# run -> CAPZ ci-entrypoint.sh -> make test-e2e -> this script's test.
# collect and cleanup run afterward, including when setup or specs fail.

set -euo pipefail
umask 077

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

fail() {
    echo "$*" >&2
    exit 1
}

[[ $# = 1 && $1 =~ ^(run|test|collect|cleanup)$ ]] || fail "Usage: ci-e2e.sh {run|test|collect|cleanup}"
: "${E2E_ROOT:?Set an absolute directory for this run}"
: "${CLUSTER_NAME:?Set a unique e2ea-prefixed cluster name}"
: "${AZURE_SUBSCRIPTION_ID:?}"
: "${AZURE_LOCATION:?}"
[[ $E2E_ROOT = /* && $CLUSTER_NAME =~ ^e2ea[a-z0-9]+$ && ${#CLUSTER_NAME} -le 36 ]] ||
    fail "Expected an absolute E2E_ROOT and an e2ea-prefixed alphanumeric cluster name (up to 36 characters)"
[[ $AZURE_LOCATION =~ ^[a-z0-9]+$ ]] || fail "Invalid Azure location"
export E2E_CLUSTER_COUNT=${E2E_CLUSTER_COUNT:-4}
[[ $E2E_CLUSTER_COUNT =~ ^([1-9]|1[0-6])$ ]] || fail "E2E_CLUSTER_COUNT must be an integer from 1 to 16"
capz=$E2E_ROOT/capz
export ARTIFACTS=$E2E_ROOT/artifacts
export KIND_CLUSTER_NAME=$CLUSTER_NAME
export MANAGEMENT_KUBECONFIG=$capz/$KIND_CLUSTER_NAME.kubeconfig
export PATH="$capz/hack/tools/bin:$PATH"
acr=${E2E_ACR_NAME:-}
clusters=("$CLUSTER_NAME")
for ((i=2; i<=E2E_CLUSTER_COUNT; i++)); do
    name=${CLUSTER_NAME}c$i
    (( ${#name} <= 36 )) || fail "Cluster names including the process suffix must fit in 36 characters"
    clusters+=("$name")
done
groups=()
for name in "${clusters[@]}"; do
    groups+=("$name" "MC_${name}_${name}_${AZURE_LOCATION}")
done
mkdir -p "$ARTIFACTS"

# --- Clients and polling ---

azs() {
    az "$@" --subscription "$AZURE_SUBSCRIPTION_ID"
}

mgmt() {
    kubectl --kubeconfig "$MANAGEMENT_KUBECONFIG" -n default "$@"
}

workload() {
    kubectl --kubeconfig "$KUBECONFIG" --request-timeout=30s "$@"
}

# --- Runner and CAPZ setup ---

check_run_environment() {
    local tool

    : "${AZURE_CLIENT_ID:?}"
    : "${KUBERNETES_VERSION:?}"
    [[ $KUBERNETES_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "Expected a numeric AKS Kubernetes version"
    for tool in az docker git go make jq; do
        command -v "$tool" >/dev/null || fail "$tool is required"
    done

    docker info >/dev/null
    [[ ! -e $E2E_ROOT/groups.txt ]] || fail "This run directory was already used"
    check_preprovisioned_resources
}

check_preprovisioned_resources() {
    local identity_id

    : "${AZURE_CONTROL_PLANE_IDENTITY_ID:?}"
    : "${AZURE_KUBELET_IDENTITY_ID:?}"
    : "${E2E_ACR_NAME:?}"

    for identity_id in "$AZURE_CONTROL_PLANE_IDENTITY_ID" "$AZURE_KUBELET_IDENTITY_ID"; do
        [[ ${identity_id,,} = "/subscriptions/${AZURE_SUBSCRIPTION_ID,,}/resourcegroups/"* ]] ||
            fail "Identity must be a UAMI resource ID in the E2E subscription: $identity_id"
        azs identity show --ids "$identity_id" --output none
    done
    azs acr show --name "$acr" --output none
}

load_capz() {
    local existing_clusters

    # shellcheck source=test/hack/capz.sh
    source "$repo/test/hack/capz.sh"
    git init -q "$capz"
    git -C "$capz" fetch -q --depth=1 https://github.com/kubernetes-sigs/cluster-api-provider-azure.git "$CAPZ_BRANCH"
    git -C "$capz" checkout -q FETCH_HEAD
    git -C "$capz" rev-parse HEAD > "$ARTIFACTS/capz-commit.txt"
    make -C "$capz" kind
    existing_clusters=$(kind get clusters) || fail "Could not inspect existing kind clusters"
    if grep -Fxq capz-e2e <<< "$existing_clusters"; then
        fail "Refusing to let CAPZ reset the existing capz-e2e kind cluster"
    fi
}

image_tag_exists() {
    local image_ref=$1 repositories tags present
    local repository=${image_ref%:*} tag=${image_ref##*:}

    repositories=$(azs acr repository list --name "$acr" --output json) || return 2
    present=$(jq -r --arg repository "$repository" 'index($repository) != null' <<< "$repositories") || return 2
    [[ $present = true ]] || return 1
    tags=$(azs acr repository show-tags --name "$acr" --repository "$repository" --output json) || return 2
    present=$(jq -r --arg tag "$tag" 'index($tag) != null' <<< "$tags") || return 2
    [[ $present = true ]]
}

record_run() {
    local group image_ref status exists
    local images=("cluster-autoscaler-$GOARCH:$TAG")

    if [[ -n ${E2E_DEALLOCATE_IMAGE:-} ]]; then
        images+=("cluster-autoscaler-deallocate:$TAG")
    fi
    for group in "${groups[@]}"; do
        exists=$(azs group exists --name "$group") || fail "Could not check resource group: $group"
        [[ $exists = false ]] || fail "Resource group already exists: $group"
    done
    for image_ref in "${images[@]}"; do
        if image_tag_exists "$image_ref"; then
            fail "Image tag already exists: $REGISTRY/$image_ref"
        else
            status=$?
            (( status == 1 )) || fail "Could not inspect image tag: $REGISTRY/$image_ref"
        fi
    done
    printf '%s\n' "${groups[@]}" > "$E2E_ROOT/groups.txt"
    : > "$E2E_ROOT/images.txt"
    for image_ref in "${images[@]}"; do
        printf '%s/%s\n' "$REGISTRY" "$image_ref" >> "$E2E_ROOT/images.txt"
    done
}

render_template() {
    local template=$1 name

    : > "$template"
    for name in "${clusters[@]}"; do
        sed -e "s/\${CLUSTER_NAME}/$name/g" \
            -e "s/\${ASO_CREDENTIAL_SECRET_NAME}/aso-credentials-$name/g" \
            "$repo/test/templates/cluster-template-prow-aks-aso-e2e.yaml" >> "$template"
        printf '\n---\n' >> "$template"
    done
}

run() {
    local REGISTRY TAG AZURE_TENANT_ID
    local GOARCH=${GOARCH:-}
    local CLUSTER_TEMPLATE=$E2E_ROOT/cluster-template-parallel-aks.yaml
    local AZWI_ENABLED=false
    local AZURE_CLIENT_ID_USER_ASSIGNED_IDENTITY
    local CLUSTER_CREATE_ATTEMPTS=1 JOB_NAME=$CLUSTER_NAME SKIP_CLEANUP=true

    check_run_environment
    GOARCH=${GOARCH:-$(go env GOARCH)}

    # CAPZ's Makefile installs its pinned kubectl, Helm, kind and kustomize.
    load_capz
    REGISTRY=$(azs acr show --name "$acr" --query loginServer --output tsv)
    TAG=$CLUSTER_NAME

    export REGISTRY TAG AZURE_TENANT_ID CLUSTER_TEMPLATE GOARCH
    record_run
    import_deallocate_image
    git -C "$repo" rev-parse HEAD > "$ARTIFACTS/source-commit.txt"
    AZURE_TENANT_ID=$(azs account show --query tenantId --output tsv)

    AZURE_CLIENT_ID_USER_ASSIGNED_IDENTITY=$AZURE_CLIENT_ID
    export AZWI_ENABLED AZURE_CLIENT_ID_USER_ASSIGNED_IDENTITY
    export AZURE_CONTROL_PLANE_IDENTITY_ID AZURE_KUBELET_IDENTITY_ID

    render_template "$CLUSTER_TEMPLATE"
    export CLUSTER_CREATE_ATTEMPTS JOB_NAME SKIP_CLEANUP
    cd "$capz"
    unset KUBECONFIG
    ARTIFACTS=$E2E_ROOT/capz-artifacts bash scripts/ci-entrypoint.sh \
        make -C "$repo/test" test-e2e \
        ARTIFACTS="$E2E_ROOT/artifacts" REGISTRY="$REGISTRY" TAG="$TAG"
}

# --- Workload bindings ---

wait_workload() {
    local deadline=$((SECONDS + 3600))

    until mgmt get "secret/$CLUSTER_NAME-kubeconfig" >/dev/null 2>&1; do
        (( SECONDS < deadline )) || fail "Workload kubeconfig did not become available: $CLUSTER_NAME"
        sleep 10
    done
    mgmt get "secret/$CLUSTER_NAME-kubeconfig" -o json |
        jq -er .data.value | base64 --decode > "$KUBECONFIG"
    mgmt wait --for=condition=Ready --timeout=60m "clusters.v1beta1.cluster.x-k8s.io/$CLUSTER_NAME"
    # Go preparation verifies the live pools after Cluster Ready.
}

write_binding() {
    local group=$1 client_id=$2 deallocate_image=

    if [[ -n ${E2E_DEALLOCATE_IMAGE:-} && $CLUSTER_NAME = "${clusters[0]}" ]]; then
        deallocate_image=${IMAGE%/*}/cluster-autoscaler-deallocate:$TAG
    fi

    jq -n --arg kubeconfig "$KUBECONFIG" --arg management "$MANAGEMENT_KUBECONFIG" \
        --arg cluster "$CLUSTER_NAME" --arg sub "$AZURE_SUBSCRIPTION_ID" \
        --arg group "$group" --arg client "$client_id" --arg image "$IMAGE:$TAG" --arg deallocate "$deallocate_image" '
        {kubeconfig:$kubeconfig,managementKubeconfig:$management,clusterName:$cluster,
         subscriptionID:$sub,resourceGroup:$group,kubeletClientID:$client,expectedImage:$image}
         + (if $deallocate == "" then {} else {deallocateImage:$deallocate} end)' > "$ARTIFACTS/environment.json"
}

bind_cluster() {
    local group client_id expected_client_id
    local node_group=MC_${CLUSTER_NAME}_${CLUSTER_NAME}_${AZURE_LOCATION}

    mgmt wait --for=condition=Ready --timeout=15m "managedclusters.containerservice.azure.com/$CLUSTER_NAME"
    group=$(mgmt get "managedclusters.containerservice.azure.com/$CLUSTER_NAME" -o json | jq -er '.status.nodeResourceGroup')
    [[ ${group,,} = "${node_group,,}" ]] || fail "Unexpected node resource group: $group"
    client_id=$(mgmt get "managedclusters.containerservice.azure.com/$CLUSTER_NAME" -o json |
        jq -er '.status.identityProfile.kubeletidentity.clientId')
    expected_client_id=$(azs identity show --ids "$AZURE_KUBELET_IDENTITY_ID" --query clientId --output tsv)
    [[ -n $expected_client_id && ${client_id,,} = "${expected_client_id,,}" ]] ||
        fail "AKS kubelet identity differs from AZURE_KUBELET_IDENTITY_ID"
    write_binding "$group" "$client_id"
}

prepare_process() {
    local index=$1
    local CLUSTER_NAME=${clusters[index-1]}
    local KUBECONFIG=$E2E_ROOT/clusters/$index/kubeconfig
    local ARTIFACTS=$E2E_ROOT/artifacts/clusters/$index

    export CLUSTER_NAME KUBECONFIG ARTIFACTS
    if (( index > 1 )); then
        wait_workload
    fi
    bind_cluster
}

import_deallocate_image() {
    if [[ -n ${E2E_DEALLOCATE_IMAGE:-} ]]; then
        azs acr import --name "$acr" \
            --source "$E2E_DEALLOCATE_IMAGE" --image "cluster-autoscaler-deallocate:$TAG" --output none
        azs acr repository show --name "$acr" --image "cluster-autoscaler-deallocate:$TAG" --query digest --output tsv \
            > "$ARTIFACTS/deallocate-image-digest.txt"
    fi
}

record_candidate() {
    azs acr repository show --name "$acr" --image "${IMAGE#*/}:$TAG" --query digest --output tsv \
        > "$ARTIFACTS/image-digest.txt"
}

# --- Bindings and suite ---

prepare_workloads() {
    local index pid result=0
    local pids=()

    for ((index=1; index<=E2E_CLUSTER_COUNT; index++)); do
        mkdir -p "$E2E_ROOT/clusters/$index" "$ARTIFACTS/clusters/$index"
        if (( index == 1 )); then
            cp "$KUBECONFIG" "$E2E_ROOT/clusters/1/kubeconfig"
        fi
        prepare_process "$index" > "$ARTIFACTS/clusters/$index/prepare.log" 2>&1 &
        pids+=("$!")
    done

    for pid in "${pids[@]}"; do
        wait "$pid" || result=1
    done

    if (( result != 0 )); then
        for ((index=1; index<=E2E_CLUSTER_COUNT; index++)); do
            echo "=== cluster $index preparation ===" >&2
            tail -60 "$ARTIFACTS/clusters/$index/prepare.log" >&2
        done
        fail "Workload cluster preparation failed"
    fi
}

run_specs() {
    local index filter=${LABEL_FILTER:-}
    local bindings=()

    for ((index=1; index<=E2E_CLUSTER_COUNT; index++)); do
        bindings+=("$ARTIFACTS/clusters/$index/environment.json")
    done
    mkdir -p "$ARTIFACTS/base"
    jq -s 'if length == 1 then .[0] else . end' "${bindings[@]}" > "$ARTIFACTS/base/environment.json"
    if [[ -z $filter && -z ${E2E_DEALLOCATE_IMAGE:-} ]]; then
        filter='!deallocate'
    fi

    make -C "$repo/test" e2etests ENVIRONMENT="$ARTIFACTS/base/environment.json" \
        LABEL_FILTER="$filter" ARTIFACTS="$ARTIFACTS/base" TEST_TIMEOUT="${TEST_TIMEOUT:-6h}"
}

test_cluster() {
    local KUBECONFIG=${KUBECONFIG:?CAPZ must provide the workload kubeconfig}

    : "${IMAGE:?}"
    : "${TAG:?}"
    [[ $TAG = "${clusters[0]}" ]] || fail "Image tag must match this run's cluster name"
    check_preprovisioned_resources
    record_candidate
    prepare_workloads
    run_specs
}

# --- Artifacts and cleanup ---

collect() {
    local index kind KUBECONFIG

    for ((index=1; index<=E2E_CLUSTER_COUNT; index++)); do
        export KUBECONFIG=$E2E_ROOT/clusters/$index/kubeconfig
        if (( index == 1 )) && [[ ! -f $KUBECONFIG ]]; then
            KUBECONFIG=$capz/kubeconfig
        fi
        [[ -f $KUBECONFIG ]] || continue
        mkdir -p "$ARTIFACTS/clusters/$index"
        for kind in nodes pods events; do
            workload get "$kind" -A -o wide > "$ARTIFACTS/clusters/$index/$kind.txt" 2>&1 ||
                echo "Could not collect $kind for cluster $index" >&2
        done
        workload -n default logs deployment/cluster-autoscaler-azure-cluster-autoscaler \
            > "$ARTIFACTS/clusters/$index/autoscaler.log" 2>&1 || echo "Could not collect autoscaler logs for cluster $index" >&2
    done
    if [[ -f $MANAGEMENT_KUBECONFIG ]]; then
        mgmt get cluster,machinepools,managedclusters.containerservice.azure.com,managedclustersagentpools.containerservice.azure.com \
            -o wide > "$ARTIFACTS/management.txt" 2>&1 || echo "Could not collect management status" >&2
    fi
}

delete_clusters() {
    local name result=0

    if [[ -f $MANAGEMENT_KUBECONFIG ]]; then
        for name in "${clusters[@]}"; do
            mgmt annotate cluster "$name" cluster.x-k8s.io/paused- ||
                echo "Could not unpause $name; attempting deletion and scoped Azure cleanup" >&2
        done
        timeout 1200 kubectl --kubeconfig "$MANAGEMENT_KUBECONFIG" -n default delete cluster "${clusters[@]}" \
            --ignore-not-found --wait=true || echo "CAPZ delete did not finish; deleting the exact groups directly" >&2
    fi
    if command -v kind >/dev/null; then
        kind delete cluster --name "$KIND_CLUSTER_NAME" ||
            {
                echo "Could not stop the kind management cluster" >&2
                result=1
            }
    fi
    return "$result"
}

delete_groups() {
    local group exists result=0

    for group in "${groups[@]}"; do
        if ! exists=$(azs group exists --name "$group"); then
            echo "Could not check resource group $group" >&2
            result=1
            continue
        fi
        if [[ $exists = true ]]; then
            azs group delete --name "$group" --yes --no-wait ||
                {
                    echo "Could not request deletion of $group" >&2
                    result=1
                }
        fi
    done
    return "$result"
}

verify_groups() {
    local group exists result=0 deadline=$((SECONDS + 1200))

    for group in "${groups[@]}"; do
        while exists=$(azs group exists --name "$group") && [[ $exists = true ]] && (( SECONDS < deadline )); do
            sleep 20
        done
        [[ $exists = false ]] || result=1
        echo "$group: exists=$exists" | tee -a "$ARTIFACTS/cleanup.txt" || exit "$?"
    done
    return "$result"
}

delete_images() {
    local image_ref reference repository registry status result=0

    [[ -f $E2E_ROOT/images.txt ]] || return 0
    registry=$(azs acr show --name "$acr" --query loginServer --output tsv) ||
        {
            echo "Could not inspect the shared ACR" >&2
            return 1
        }

    while IFS= read -r image_ref; do
        reference=${image_ref#"$registry/"}
        repository=${reference%:*}
        if [[ $image_ref != "$registry/"* || ${reference##*:} != "${clusters[0]}" ||
            ! $repository =~ ^cluster-autoscaler-[a-z0-9]+$ ]]; then
            echo "Image tag is outside this run: $image_ref" >&2
            result=1
            continue
        fi

        if image_tag_exists "$reference"; then
            if ! azs acr repository untag --name "$acr" --image "$reference" --output none; then
                echo "Could not delete image tag $image_ref" >&2
                result=1
                continue
            fi
        else
            status=$?
            if (( status != 1 )); then
                echo "Could not inspect image tag $image_ref" >&2
                result=1
                continue
            fi
        fi

        if image_tag_exists "$reference"; then
            echo "Image tag still exists: $image_ref" >&2
            result=1
        else
            status=$?
            if (( status != 1 )); then
                echo "Could not verify image tag deletion: $image_ref" >&2
                result=1
            fi
        fi
    done < "$E2E_ROOT/images.txt" || exit "$?"

    return "$result"
}

cleanup() {
    local result=0

    [[ -f $E2E_ROOT/groups.txt ]] || return 0
    diff -u "$E2E_ROOT/groups.txt" <(printf '%s\n' "${groups[@]}") ||
        fail "Resource group manifest does not match this run"

    delete_images || result=1
    delete_clusters || result=1
    delete_groups || result=1
    verify_groups || result=1

    return "$result"
}

case $1 in
    run) run ;;
    test) test_cluster ;;
    collect) collect ;;
    cleanup) cleanup ;;
esac
