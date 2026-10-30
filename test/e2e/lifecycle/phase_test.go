//go:build e2e

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

package lifecycle

import (
	"testing"
)

func TestCheckBalancedPlan(t *testing.T) {
	for _, tc := range []struct {
		name, logs string
		valid      bool
	}{
		{
			"one-plan",
			`Final scale-up plan scaleUpInfos="[{a 0->1 (max: 2)} {b 0->1 (max: 2)}]"`,
			true,
		},
		{
			"reverse",
			`Final scale-up plan scaleUpInfos="[{b 0->1 (max: 2)} {a 0->1 (max: 2)}]"`,
			true,
		},
		{
			"separate-plans",
			"Final scale-up plan [{a 0->1 (max: 2)}]\nFinal scale-up plan [{b 0->1 (max: 2)}]",
			false,
		},
		{
			"wrong-pool",
			"Final scale-up plan [{a 0->1 (max: 2)} {c 0->1 (max: 2)}]",
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkBalancedPlan(tc.logs, "a", "b"); (err == nil) != tc.valid {
				t.Fatalf("checkBalancedPlan()=%v, valid=%t", err, tc.valid)
			}
		})
	}
}
