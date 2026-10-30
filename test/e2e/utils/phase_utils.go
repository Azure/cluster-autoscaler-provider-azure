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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v8"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (e *Environment) setPhase(phase string) {
	e.Config.Preparation = phase
	e.azure.config = e.Config
}

// PreparePhase changes the bound cluster's pools, tags and controller for phase.
func (e *Environment) PreparePhase(ctx context.Context, phase string) error {
	e.setPhase(phase)

	// Stop the controller before changing the fixture.
	if err := e.scaleController(ctx, 0); err != nil {
		return err
	}

	switch phase {
	case TestSuiteLabelTaint:
		if err := e.updateNodePool(ctx, "zero", func(pool *armcontainerservice.AgentPool) {
			pool.Properties.NodeTaints = []*string{
				ptr.To("acceptance-pool=zero:NoSchedule"), ptr.To(RunLabel + "=" + e.Config.RunID + ":NoSchedule"),
			}
		}); err != nil {
			return err
		}
		if err := e.updateTags(ctx, e.Config.ZeroPool, armresources.TagsPatchOperationMerge,
			map[string]*string{ZeroPoolTaintTag: ptr.To(e.Config.RunID + ":NoSchedule")}); err != nil {
			return err
		}
		return e.scaleController(ctx, 1)
	case TestSuiteLabelMinimum:
		if err := wait.PollUntilContextTimeout(ctx, PollInterval, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
			if err := e.NoPendingDemand(ctx); err != nil {
				Logf("Waiting for pending demand to clear: %v", err)
				return false, nil
			}
			return true, nil
		}); err != nil {
			return err
		}
		return e.updateTags(ctx, e.Config.MainPool, armresources.TagsPatchOperationMerge, map[string]*string{"min": ptr.To("2")})
	case TestSuiteLabelBalance:
		return e.command(ctx, "", "kubectl", "wait", "-n", AutoscalerNamespace, "--for=delete", "pod",
			"-l", "app.kubernetes.io/instance=cluster-autoscaler", "--timeout=3m")
	case TestSuiteLabelNoJoin:
		return e.InstallController(ctx, e.Config.ExpectedImage, false)
	case TestSuiteLabelDeallocate:
		if e.Config.DeallocateImage == "" {
			return fmt.Errorf("deallocate requires deallocateImage")
		}
		if err := e.updateNodePool(ctx, "main", func(pool *armcontainerservice.AgentPool) {
			pool.Properties.ScaleDownMode = ptr.To(armcontainerservice.ScaleDownModeDeallocate)
		}); err != nil {
			return err
		}
		if err := e.updateTags(
			ctx,
			e.Config.MainPool,
			armresources.TagsPatchOperationMerge,
			map[string]*string{"max": ptr.To("3")},
		); err != nil {
			return err
		}
		if err := e.waitForSettlement(ctx); err != nil {
			return err
		}
		e.Config.ExpectedImage = e.Config.DeallocateImage
		e.setPhase(phase)
		return e.InstallController(ctx, e.Config.DeallocateImage, true)
	default:
		return fmt.Errorf("unknown preparation %q", phase)
	}
}

// RestorePhase undoes the taint and minimum changes.
func (e *Environment) RestorePhase(ctx context.Context, phase string) error {
	switch phase {
	case TestSuiteLabelTaint:
		if err := e.scaleController(ctx, 0); err != nil {
			return err
		}
		if err := e.updateNodePool(ctx, "zero", func(pool *armcontainerservice.AgentPool) {
			pool.Properties.NodeTaints = []*string{ptr.To("acceptance-pool=zero:NoSchedule")}
		}); err != nil {
			return err
		}
		return e.updateTags(ctx, e.Config.ZeroPool, armresources.TagsPatchOperationDelete, map[string]*string{ZeroPoolTaintTag: nil})
	case TestSuiteLabelMinimum:
		if err := e.scaleController(ctx, 0); err != nil {
			return err
		}
		if err := e.updateTags(ctx, e.Config.MainPool, armresources.TagsPatchOperationMerge, map[string]*string{"min": ptr.To("1")}); err != nil {
			return err
		}
		if err := e.scaleController(ctx, 1); err != nil {
			return err
		}
		return wait.PollUntilContextTimeout(ctx, PollInterval, SettleTimeout, true, func(ctx context.Context) (bool, error) {
			snapshot, err := e.Read(ctx)
			if err != nil {
				Logf("Could not read the baseline: %v", err)
				return false, nil
			}
			if err := snapshot.Stable(e.Config, 1, 0); err != nil {
				Logf("Waiting for the baseline: %v", err)
				return false, nil
			}
			return true, nil
		})
	}

	return nil
}

func (e *Environment) scaleController(ctx context.Context, replicas int32) error {
	if replicas == 0 {
		e.SaveControllerLogs(ctx)
	}

	var deployment appsv1.Deployment
	if err := e.K8s.Get(ctx, client.ObjectKey{Namespace: AutoscalerNamespace, Name: AutoscalerDeployment}, &deployment); err != nil {
		return err
	}

	before := deployment.DeepCopy()
	deployment.Spec.Replicas = ptr.To(replicas)

	return e.K8s.Patch(ctx, &deployment, client.MergeFrom(before))
}

func (e *Environment) updateTags(ctx context.Context, pool string, operation armresources.TagsPatchOperation, tags map[string]*string) error {
	_, err := e.azure.tags.UpdateAtScope(ctx, e.Config.PoolID(pool),
		armresources.TagsPatchResource{Operation: &operation, Properties: &armresources.Tags{Tags: tags}}, nil)
	if err != nil {
		return azureError("update fixture tags", err)
	}

	return nil
}

func (e *Environment) updateNodePool(ctx context.Context, name string, update func(*armcontainerservice.AgentPool)) error {
	c := e.Config
	pool, err := e.azure.pools.Get(ctx, c.ClusterName, c.ClusterName, name, nil)
	if err != nil {
		return azureError("read fixture node pool", err)
	}

	if pool.Properties == nil {
		return fmt.Errorf("node pool %s has no properties", name)
	}

	update(&pool.AgentPool)
	poller, err := e.azure.pools.BeginCreateOrUpdate(ctx, c.ClusterName, c.ClusterName, name, pool.AgentPool, nil)
	if err != nil {
		return azureError("update fixture node pool", err)
	}

	if _, err := poller.PollUntilDone(ctx, nil); err != nil {
		return azureError("wait for fixture node pool update", err)
	}

	return nil
}
