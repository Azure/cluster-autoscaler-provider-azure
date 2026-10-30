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
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestControllerImageTag(t *testing.T) {
	t.Parallel()
	for _, tag := range []string{"20261030", "1234567", "e2eatest"} {
		t.Run(tag, func(t *testing.T) {
			var values strings.Builder
			if err := controllerValuesTemplate.Execute(&values, controllerValues{ImageTag: tag}); err != nil {
				t.Fatal(err)
			}

			var parsed map[string]any
			if err := yaml.Unmarshal([]byte(values.String()), &parsed); err != nil {
				t.Fatal(err)
			}

			image, ok := parsed["image"].(map[string]any)
			if !ok || image["tag"] != tag {
				t.Fatalf("image tag did not remain a string: %#v", parsed["image"])
			}
		})
	}
}
