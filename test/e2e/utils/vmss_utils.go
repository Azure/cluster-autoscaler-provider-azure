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

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
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

func (a *AzureTestClient) listSets(ctx context.Context) ([]*armcompute.VirtualMachineScaleSet, error) {
	sets := []*armcompute.VirtualMachineScaleSet{}
	pager := a.sets.NewListPager(a.config.ResourceGroup, nil)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, azureError("list fixture VMSS", err)
		}

		sets = append(sets, page.Value...)
	}

	return sets, nil
}

func poolsFromTags(sets []*armcompute.VirtualMachineScaleSet) (map[string]string, error) {
	pools := map[string]string{}

	for _, set := range sets {
		if set == nil || set.Name == nil {
			return nil, fmt.Errorf("fixture VMSS has no name")
		}

		pool := ptr.Deref(set.Tags["aks-managed-poolName"], "")
		if pool == "" || pools[pool] != "" {
			return nil, fmt.Errorf("missing or duplicate AKS pool tag %q", pool)
		}

		pools[pool] = *set.Name
	}

	for _, pool := range []string{
		"pool0",
		"main",
		"zero",
	} {
		if pools[pool] == "" {
			return nil, fmt.Errorf("fixture pool %s is missing", pool)
		}
	}

	if len(pools) != 3 {
		return nil, fmt.Errorf("expected three fixture pools, got %d", len(pools))
	}

	return pools, nil
}

func (e *Environment) findPools(sets []*armcompute.VirtualMachineScaleSet) error {
	pools, err := poolsFromTags(sets)
	if err != nil {
		return err
	}

	e.Config.MainPool, e.Config.ZeroPool = pools["main"], pools["zero"]
	e.azure.config = e.Config

	return nil
}
