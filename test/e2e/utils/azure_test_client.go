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
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v8"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
)

// Cloud is the read-only Azure view that the tests use.
type Cloud interface {
	Read(context.Context) (Snapshot, error)
	NICExists(context.Context, string) (bool, error)
}

// AzureTestClient holds the SDK clients for the bound subscription.
type AzureTestClient struct {
	config   Config
	sets     *armcompute.VirtualMachineScaleSetsClient
	vms      *armcompute.VirtualMachineScaleSetVMsClient
	nics     *armnetwork.InterfacesClient
	clusters *armcontainerservice.ManagedClustersClient
	pools    *armcontainerservice.AgentPoolsClient
	tags     *armresources.TagsClient
}

func newCloud(cfg Config) (*AzureTestClient, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("initialize Azure credential: %w", err)
	}

	factory, err := armcompute.NewClientFactory(cfg.SubscriptionID, credential, nil)
	if err != nil {
		return nil, err
	}

	aks, err := armcontainerservice.NewClientFactory(cfg.SubscriptionID, credential, nil)
	if err != nil {
		return nil, err
	}

	tags, err := armresources.NewTagsClient(cfg.SubscriptionID, credential, nil)
	if err != nil {
		return nil, err
	}

	nics, err := armnetwork.NewInterfacesClient(cfg.SubscriptionID, credential, nil)
	if err != nil {
		return nil, err
	}

	return &AzureTestClient{
		config:   cfg,
		sets:     factory.NewVirtualMachineScaleSetsClient(),
		vms:      factory.NewVirtualMachineScaleSetVMsClient(),
		nics:     nics,
		clusters: aks.NewManagedClustersClient(),
		pools:    aks.NewAgentPoolsClient(),
		tags:     tags,
	}, nil
}

// NICExists reports whether the VMSS VM NIC with id exists.
func (a *AzureTestClient) NICExists(ctx context.Context, id string) (bool, error) {
	resource, err := arm.ParseResourceID(id)
	if err != nil {
		return false, fmt.Errorf("parse NIC ID: %w", err)
	}

	if resource.Parent == nil || resource.Parent.Parent == nil {
		return false, fmt.Errorf("NIC ID must name a VMSS VM NIC")
	}

	_, err = a.nics.GetVirtualMachineScaleSetNetworkInterface(
		ctx,
		resource.ResourceGroupName,
		resource.Parent.Parent.Name,
		resource.Parent.Name,
		resource.Name,
		nil,
	)
	var response *azcore.ResponseError

	if errors.As(err, &response) && response.StatusCode == http.StatusNotFound {
		return false, nil
	}

	if err != nil {
		return false, azureError("read NIC", err)
	}

	return true, nil
}

// azureError adds operation to err. An Azure response error keeps only its
// status and error code, because its body can contain VM bootstrap data.
func azureError(operation string, err error) error {
	var response *azcore.ResponseError

	if errors.As(err, &response) {
		return fmt.Errorf("%s failed: HTTP %d code=%s", operation, response.StatusCode, response.ErrorCode)
	}

	return fmt.Errorf("%s failed: %w", operation, err)
}
