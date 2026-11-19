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
	"fmt"
	"regexp"
	"strings"
)

var balancedPlanRE = regexp.MustCompile(`\[\{([^{}]+) 0->1 \(max: 2\)\} \{([^{}]+) 0->1 \(max: 2\)\}\]`)

func checkBalancedPlan(logs, a, b string) error {
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, "Final scale-up plan") {
			continue
		}

		groups := balancedPlanRE.FindStringSubmatch(line)
		if groups != nil && ((groups[1] == a && groups[2] == b) || (groups[1] == b && groups[2] == a)) {
			return nil
		}
	}

	return fmt.Errorf("no single plan added one node to each balance pool")
}
