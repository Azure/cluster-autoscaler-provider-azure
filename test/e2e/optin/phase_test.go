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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestScaleUpTimedOutSince(t *testing.T) {
	started := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name        string
		reason      string
		pool        string
		last, event time.Time
		want        bool
	}{
		{
			name:   "fresh last timestamp",
			reason: "ScaleUpTimedOut",
			pool:   "zero",
			last:   started.Add(time.Second),
			want:   true,
		},
		{
			name:   "fresh event time",
			reason: "ScaleUpTimedOut",
			pool:   "zero",
			event:  started.Add(time.Second),
			want:   true,
		},
		{
			name:   "previous attempt",
			reason: "ScaleUpTimedOut",
			pool:   "zero",
			last:   started.Add(-time.Second),
		},
		{
			name:   "at start",
			reason: "ScaleUpTimedOut",
			pool:   "zero",
			last:   started,
		},
		{
			name:   "missing timestamp",
			reason: "ScaleUpTimedOut",
			pool:   "zero",
		},
		{
			name:   "different pool",
			reason: "ScaleUpTimedOut",
			pool:   "main",
			last:   started.Add(time.Second),
		},
		{
			name:   "different reason",
			reason: "DeleteUnregistered",
			pool:   "zero",
			last:   started.Add(time.Second),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := []corev1.Event{{
				Reason:        tc.reason,
				Message:       "Nodes added to group " + tc.pool + " failed to register",
				LastTimestamp: metav1.NewTime(tc.last),
				EventTime:     metav1.NewMicroTime(tc.event),
			}}
			if got := scaleUpTimedOutSince(events, "zero", started); got != tc.want {
				t.Fatalf("scaleUpTimedOutSince() = %t, want %t", got, tc.want)
			}
		})
	}
}
