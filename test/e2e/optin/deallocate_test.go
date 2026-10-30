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

package optin

import (
	"testing"
	"time"
)

func TestCheckParkedFirst(t *testing.T) {
	at := time.Now()

	for _, tc := range []struct {
		name          string
		start, growth time.Time
		valid         bool
	}{
		{
			"start-first",
			at,
			at.Add(time.Second),
			true,
		},
		{
			"growth-first",
			at.Add(time.Second),
			at,
			false,
		},
		{
			"same-time",
			at,
			at,
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operations := []azureOperation{
				{
					At:        tc.start,
					Resource:  "pool/virtualmachines/0",
					Operation: "Microsoft.Compute/virtualMachineScaleSets/virtualMachines/start/action",
					Status:    "Started",
					ID:        "start",
				},
				{
					At:        tc.growth,
					Resource:  "pool",
					Operation: "Microsoft.Compute/virtualMachineScaleSets/write",
					Status:    "Started",
					ID:        "grow",
				},
			}
			if err := checkParkedFirst(operations, "pool", "pool/virtualmachines/0"); (err == nil) != tc.valid {
				t.Fatalf("checkParkedFirst()=%v, valid=%t", err, tc.valid)
			}
		})
	}
}
