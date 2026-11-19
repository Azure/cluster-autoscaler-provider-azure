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
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
	"k8s.io/utils/ptr"
)

func TestPoolsFromTags(t *testing.T) {
	sets := []*armcompute.VirtualMachineScaleSet{}

	for _, name := range []string{
		"pool0",
		"main",
		"zero",
	} {
		sets = append(sets, &armcompute.VirtualMachineScaleSet{
			Name: ptr.To("vmss-" + name), Tags: map[string]*string{"aks-managed-poolName": ptr.To(name)},
		})
	}

	pools, err := poolsFromTags(sets)
	if err != nil || pools["main"] != "vmss-main" || pools["zero"] != "vmss-zero" {
		t.Fatalf("pool discovery = %v, %v", pools, err)
	}

	if _, err := poolsFromTags(sets[1:]); err == nil {
		t.Fatal("accepted a missing pool")
	}

	if _, err := poolsFromTags(append(sets, sets[0])); err == nil {
		t.Fatal("accepted duplicate pool tags")
	}
}
