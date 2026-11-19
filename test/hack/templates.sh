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

set -euo pipefail

[[ $# = 1 && $1 =~ ^(generate|verify)$ ]] || {
    echo "Usage: templates.sh {generate|verify}" >&2
    exit 1
}
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=test/hack/capz.sh
source "$repo/test/hack/capz.sh"

capz=$repo/test/.tools/capz-$CAPZ_VERSION
git init -q "$capz"
if ! git -C "$capz" show-ref --verify --quiet "refs/tags/$CAPZ_VERSION"; then
    git -C "$capz" fetch -q --depth=1 https://github.com/kubernetes-sigs/cluster-api-provider-azure.git \
        "refs/tags/$CAPZ_VERSION:refs/tags/$CAPZ_VERSION"
fi
git -C "$capz" checkout -q --detach "$CAPZ_VERSION"

overlay=$capz/templates/test/autoscaler-e2e
mkdir -p "$overlay"
cp -R "$repo/test/templates/aks-aso-e2e/." "$overlay/"
make --no-print-directory -C "$capz" kustomize
kustomize=$capz/hack/tools/bin/kustomize

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
{
    sed -n '1,/^$/p' "$overlay/kustomization.yaml"
    printf '%s\n' '# Generated from templates/aks-aso-e2e; run make -C test generate-templates.'
    "$kustomize" build --load-restrictor LoadRestrictionsNone "$overlay"
} > "$tmp"
output=$repo/test/templates/cluster-template-prow-aks-aso-e2e.yaml
case $1 in
    generate) mv "$tmp" "$output" ;;
    verify) diff -u "$output" "$tmp" ;;
esac
