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
	"fmt"
	"strconv"

	"k8s.io/utils/ptr"
)

// Read returns the configured VMSS with their VMs and NICs.
func (a *AzureTestClient) Read(ctx context.Context) (Snapshot, error) {
	c := a.config
	result := Snapshot{Pools: map[string]PoolState{}}

	for _, name := range c.Pools() {
		response, err := a.sets.Get(ctx, c.ResourceGroup, name, nil)
		if err != nil {
			return result, azureError("read VMSS "+name, err)
		}

		set := response.VirtualMachineScaleSet
		if set.SKU == nil || set.SKU.Capacity == nil || set.Properties == nil {
			return result, fmt.Errorf("incomplete VMSS %s observation", name)
		}

		maximum, err := strconv.Atoi(ptr.Deref(set.Tags["max"], ""))
		if err != nil {
			return result, fmt.Errorf("VMSS %s has no numeric max tag", name)
		}

		pool := PoolState{
			Capacity:          int(*set.SKU.Capacity),
			Max:               maximum,
			ProvisioningState: ptr.Deref(set.Properties.ProvisioningState, ""),
			Instances:         map[string]Instance{},
		}
		pager := a.vms.NewListPager(c.ResourceGroup, name, nil)

		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return result, azureError("list VMSS instances", err)
			}

			for _, vm := range page.Value {
				if vm == nil || vm.ID == nil || vm.Properties == nil ||
					vm.Properties.NetworkProfile == nil || len(vm.Properties.NetworkProfile.NetworkInterfaces) == 0 {
					return result, fmt.Errorf("VMSS %s instance lacks identity or network evidence", name)
				}

				instance := Instance{
					ID:                normalizeID(*vm.ID),
					ProvisioningState: ptr.Deref(vm.Properties.ProvisioningState, ""),
				}

				for _, nic := range vm.Properties.NetworkProfile.NetworkInterfaces {
					if nic == nil || nic.ID == nil {
						return result, fmt.Errorf("VMSS %s instance lacks NIC ID", name)
					}

					instance.NICs = append(instance.NICs, *nic.ID)
				}

				pool.Instances[instance.ID] = instance
			}
		}

		result.Pools[name] = pool
	}

	return result, nil
}
