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
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/fake"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
)

type sdkTransport func(*http.Request) (*http.Response, error)

func (f sdkTransport) Do(request *http.Request) (*http.Response, error) { return f(request) }

func TestAzureCloudNICExists(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		status    int
		transport error
		exists    bool
		wantError bool
	}{
		{
			name:   "NIC exists",
			status: http.StatusOK,
			exists: true,
		},
		{name: "NIC deleted", status: http.StatusNotFound},
		{
			name:      "authorization error is not absence",
			status:    http.StatusForbidden,
			wantError: true,
		},
		{
			name:      "transport error is not absence",
			transport: errors.New("transport unavailable"),
			wantError: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig()
			nicID := c.PoolID(c.MainPool) + "/virtualMachines/0/networkInterfaces/nic"
			nics, err := armnetwork.NewInterfacesClient(c.SubscriptionID, &fake.TokenCredential{}, &arm.ClientOptions{
				ClientOptions: azcore.ClientOptions{
					Retry: policy.RetryOptions{MaxRetries: -1},
					Transport: sdkTransport(func(request *http.Request) (*http.Response, error) {
						if request.Method != http.MethodGet || !strings.EqualFold(request.URL.Path, nicID) {
							t.Fatalf("unexpected NIC request: %s %s", request.Method, request.URL)
						}

						if tt.transport != nil {
							return nil, tt.transport
						}

						return &http.Response{
							StatusCode: tt.status,
							Header:     http.Header{"Content-Type": []string{"application/json"}},
							Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"Denied","message":"bootstrap-secret"}}`)),
							Request:    request,
						}, nil
					}),
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			exists, err := (&AzureTestClient{config: c, nics: nics}).NICExists(context.Background(), nicID)
			if exists != tt.exists || (err != nil) != tt.wantError {
				t.Fatalf("NICExists() exists=%t, error=%v; want exists=%t, error=%t", exists, err, tt.exists, tt.wantError)
			}

			if err != nil && strings.Contains(err.Error(), "bootstrap-secret") {
				t.Fatal("NIC error exposed the Azure response body")
			}
		})
	}

	c := testConfig()

	for _, id := range []string{
		"not-a-resource-id",
		"/subscriptions/sub",
	} {
		if _, err := (&AzureTestClient{config: c}).NICExists(context.Background(), id); err == nil {
			t.Fatalf("accepted invalid or unowned NIC ID: %s", id)
		}
	}
}
