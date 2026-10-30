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

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestDefaultSpecFilter(t *testing.T) {
	t.Parallel()
	script, err := os.ReadFile(repoFile("test/hack/ci-e2e.sh"))
	if err != nil {
		t.Fatal(err)
	}

	_, function, ok := strings.Cut(string(script), "run_specs() {")
	if !ok {
		t.Fatal("run_specs not found")
	}
	function, _, ok = strings.Cut(function, "\n}\n")
	if !ok {
		t.Fatal("run_specs end not found")
	}

	for _, tt := range []struct{ name, image, filter, want string }{
		{name: "never-registers by default", want: "!deallocate"},
		{name: "deallocate image enables all", image: "registry/repo:tag"},
		{name: "explicit selection", filter: "no-join", want: "no-join"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Stub only the tools that would launch the real suite or read bindings.
			command := exec.Command("bash", "-eu", "-c", "run_specs() {"+function+`
}
make() { printf '%s\n' "$@"; }
jq() { printf '{}'; }
repo=unused
run_specs
`)
			command.Env = append(os.Environ(), "ARTIFACTS="+t.TempDir(), "E2E_CLUSTER_COUNT=1",
				"E2E_DEALLOCATE_IMAGE="+tt.image, "LABEL_FILTER="+tt.filter)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("run_specs: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "\nLABEL_FILTER="+tt.want+"\n") {
				t.Fatalf("unexpected suite filter: %s", output)
			}
		})
	}
}
