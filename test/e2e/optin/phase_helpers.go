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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func scaleUpTimedOutSince(events []corev1.Event, pool string, started time.Time) bool {
	for _, event := range events {
		if event.Reason == "ScaleUpTimedOut" && strings.Contains(event.Message, pool) &&
			(event.LastTimestamp.Time.After(started) || event.EventTime.Time.After(started)) {
			return true
		}
	}

	return false
}
