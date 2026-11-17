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

package azure

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/taints"
)

const (
	suspendedCondition               apiv1.NodeConditionType = "Suspended"
	suspendedTaintKey                                        = taints.StatusTaintPrefix + "azure-suspended"
	scaleDownDisabledAnnotation                              = "cluster-autoscaler.kubernetes.io/scale-down-disabled"
	scaleDownDisabledOwnerAnnotation                         = "cluster-autoscaler.kubernetes.io/azure-owns-scale-down-disabled"
	parkedReason                                             = "AzureVMDeallocated"
	resumedReason                                            = "AzureVMStarted"
)

var errNodeChangedIdentity = errors.New("changed identity")

type suspendedParkOperation struct {
	sequence uint64
	vmID     string
	poller   *runtime.Poller[armcompute.VirtualMachineScaleSetVMsClientDeallocateResponse]
}

// deleteSuspendedNodes parks real Nodes and physically deletes the synthetic Nodes core passes for
// unregistered or failed VMs. enforceMinimum keeps the running minimum, as DeleteNodes does.
func (s *ScaleSet) deleteSuspendedNodes(ctx context.Context, nodes []*apiv1.Node, enforceMinimum bool) error {
	if s.manager.kubeClient == nil {
		return fmt.Errorf("deallocate mode requires a Kubernetes client")
	}

	ctx, cancel := context.WithTimeout(ctx, asyncContextTimeout)
	defer cancel()
	// Split real Nodes to park from synthetic Nodes to delete.
	parkNodes := make([]*apiv1.Node, 0, len(nodes))
	syntheticNodes := make([]*apiv1.Node, 0, len(nodes))
	for _, node := range nodes {
		reason := node.Annotations[cloudprovider.FakeNodeReasonAnnotation]
		isSynthetic := node.UID == "" &&
			(reason == cloudprovider.FakeNodeUnregistered || reason == cloudprovider.FakeNodeCreateError)
		if isSynthetic {
			syntheticNodes = append(syntheticNodes, node)
			continue
		}

		parkNodes = append(parkNodes, node)
	}

	if len(parkNodes) > 0 {
		if err := s.parkSuspendedNodes(ctx, parkNodes, enforceMinimum); err != nil {
			return err
		}
	}

	// Refuse registered instances under the park lock, so cleanup never deletes a parked VM.
	if len(syntheticNodes) > 0 {
		if err := s.lockParkOperation(ctx); err != nil {
			return err
		}

		defer s.parkMutex.Unlock()
		if enforceMinimum {
			size, err := s.getScaleSetSize()
			if err != nil {
				return err
			}
			if int(size) <= s.MinSize(ctx) {
				return fmt.Errorf("min size reached, nodes will not be deleted")
			}
		}

		currentNodes, err := s.manager.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("recheck nodes before synthetic cleanup: %w", err)
		}

		for _, current := range currentNodes.Items {
			for _, node := range syntheticNodes {
				if normalizeProviderID(current.Spec.ProviderID) == normalizeProviderID(node.Spec.ProviderID) {
					return fmt.Errorf("refusing synthetic cleanup of registered instance %s", node.Spec.ProviderID)
				}
			}
		}
		return s.forceDeleteNodes(ctx, syntheticNodes)
	}

	return nil
}

// parkSuspendedNodes sends a Deallocate for each Node and waits for every accepted one to finish.
func (s *ScaleSet) parkSuspendedNodes(ctx context.Context, nodes []*apiv1.Node, enforceMinimum bool) error {
	if s.manager.kubeClient == nil || s.manager.azClient.vmssPowerClient == nil {
		return fmt.Errorf("deallocate mode requires Kubernetes and VMSS power clients")
	}

	accepted, err := s.submitSuspendedParking(ctx, nodes, enforceMinimum)
	for _, operation := range accepted {
		if finishErr := s.finishSuspendedParking(ctx, operation); finishErr != nil {
			err = errors.Join(err, finishErr)
		}
	}
	return err
}

// submitSuspendedParking checks the Nodes and sends a Deallocate for each VM that isn't parked yet.
// It returns the accepted operations, even when a later submission fails.
func (s *ScaleSet) submitSuspendedParking(ctx context.Context, nodes []*apiv1.Node, enforceMinimum bool) ([]suspendedParkOperation, error) {
	if err := s.lockParkOperation(ctx); err != nil {
		return nil, err
	}
	defer s.parkMutex.Unlock()
	vms, err := s.suspendedInventory(ctx, true)
	if err != nil {
		return nil, err
	}

	// Other VMs' operations must not block this scale-down; pending ones are refused below.
	if err := s.reconcileSuspendedPower(ctx, vms); err != nil {
		klog.Warningf("Failed to reconcile power operations in %s: %v", s.Name, err)
	}

	byProviderID := make(map[string]*armcompute.VirtualMachineScaleSetVM, len(vms))
	for _, vm := range vms {
		byProviderID[normalizeProviderID(azurePrefix+*vm.ID)] = vm
	}

	// Check every Node before sending anything: it must be a VM in this group with no pending
	// operation and a stable power state.
	var newParks int
	for _, node := range nodes {
		vm := byProviderID[normalizeProviderID(node.Spec.ProviderID)]
		if vm == nil {
			return nil, fmt.Errorf("node %s is not a VM in %s", node.Name, s.Name)
		}
		if err := s.checkNodeIdentity(ctx, node); err != nil {
			return nil, err
		}

		op, pending := s.powerOperation(*vm.Properties.VMID)
		if pending && (op.polling || !op.park || !op.failed && !op.completed) {
			return nil, fmt.Errorf("power operation pending for %s", node.Name)
		}
		state := suspendedPowerState(vm)
		parked := s.effectivelyParked(vm)
		stablePower := state == vmPowerStateRunning || state == vmPowerStateStopped || state == vmPowerStateDeallocated
		if !parked && !stablePower {
			return nil, fmt.Errorf("cannot park node %s with power state %s", node.Name, state)
		}
		if !parked {
			newParks++
		}
	}

	// Refuse parks that would leave fewer running VMs than the configured minimum.
	if enforceMinimum && newParks > 0 {
		total, err := s.getScaleSetSize()
		if err != nil {
			return nil, err
		}

		running := int(total) - s.parkedCount()
		if running-newParks < s.minSize {
			return nil, fmt.Errorf("min size reached: parking %d nodes leaves %d running, minimum %d",
				newParks, running-newParks, s.minSize)
		}
	}

	// Record already parked VMs as done and send a Deallocate for the rest.
	accepted := make([]suspendedParkOperation, 0, len(nodes))
	for _, node := range nodes {
		vm := byProviderID[normalizeProviderID(node.Spec.ProviderID)]
		op := suspendedParkOperation{vmID: *vm.Properties.VMID}
		parked := s.effectivelyParked(vm)
		if parked {
			s.recordPower(op.vmID, suspendedPowerOperation{node: node.DeepCopy(), park: true, completed: true})
			accepted = append(accepted, op)
			continue
		}

		submitCtx, cancel := context.WithTimeout(ctx, vmssContextTimeout)
		poller, err := s.manager.azClient.vmssPowerClient.BeginDeallocate(
			submitCtx, s.manager.config.ResourceGroup, s.Name, *vm.InstanceID, nil)
		cancel()
		if err != nil && isDefinitePowerRejection(err) {
			return accepted, fmt.Errorf("deallocate %s: %w", node.Name, err)
		}

		op.sequence = s.recordPower(op.vmID, suspendedPowerOperation{node: node.DeepCopy(), park: true, polling: poller != nil})
		if poller != nil {
			op.poller = poller
			accepted = append(accepted, op)
		}
		if err != nil || poller == nil {
			if err == nil {
				err = fmt.Errorf("no operation returned")
			}
			// Read Azure again before settling, since the cached view predates this request.
			s.invalidateSuspendedInventory()
			return accepted, fmt.Errorf("deallocate %s has uncertain acceptance: %w", node.Name, err)
		}
	}

	return accepted, nil
}

// finishSuspendedParking waits for an accepted Deallocate, then settles it from a fresh inventory.
func (s *ScaleSet) finishSuspendedParking(ctx context.Context, operation suspendedParkOperation) error {
	if operation.poller != nil {
		_, err := operation.poller.PollUntilDone(ctx, nil)
		s.finishPowerPolling(operation.vmID, operation.sequence, err)
		s.invalidateSuspendedInventory()
		if err != nil {
			return fmt.Errorf("waiting for deallocate %s: %w", operation.vmID, err)
		}
	}

	if err := s.lockParkOperation(ctx); err != nil {
		return err
	}
	defer s.parkMutex.Unlock()
	vms, err := s.suspendedInventory(ctx, true)
	if err != nil {
		return err
	}

	for _, vm := range vms {
		if *vm.Properties.VMID == operation.vmID {
			return s.reconcileSuspendedPower(ctx, []*armcompute.VirtualMachineScaleSetVM{vm})
		}
	}
	return fmt.Errorf("VM %s changed identity during deallocation", operation.vmID)
}

// increaseSuspended grows capacity without starting parked VMs.
func (s *ScaleSet) increaseSuspended(ctx context.Context, delta int) error {
	if delta <= 0 {
		return fmt.Errorf("size increase must be positive")
	}

	ctx, cancel := context.WithTimeout(ctx, vmssContextTimeout)
	defer cancel()
	if s.manager.kubeClient == nil || s.manager.azClient.vmssPowerClient == nil {
		return fmt.Errorf("deallocate mode requires Kubernetes and VMSS power clients")
	}
	if err := s.lockParkOperation(ctx); err != nil {
		return err
	}
	defer s.parkMutex.Unlock()
	vms, err := s.suspendedInventory(ctx, true)
	if err != nil {
		return err
	}

	// Settle other operations without letting their errors block the scale-up, then check the maximum.
	if err := s.reconcileSuspendedPower(ctx, vms); err != nil {
		klog.Warningf("Failed to reconcile power operations in %s: %v", s.Name, err)
	}
	physical, err := s.canIncreaseSize(delta)
	if err != nil {
		return err
	}

	// Only confirmed parks provide headroom above the configured maximum.
	unconfirmed := 0
	for _, vm := range vms {
		op, pending := s.powerOperation(*vm.Properties.VMID)
		if pending && op.park && !op.completed && !op.failed {
			unconfirmed++
		}
	}
	if int(physical)+delta > s.maxSize+s.parkedCount()-unconfirmed {
		return fmt.Errorf("parking is still in progress for %s; refusing replacement growth", s.Name)
	}
	if err := s.setScaleSetSize(physical+int64(delta), delta); err != nil {
		return err
	}

	return nil
}

func (s *ScaleSet) checkNodeIdentity(ctx context.Context, node *apiv1.Node) error {
	current, err := s.manager.kubeClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	return sameSuspendedNode(current, node)
}

// reconcileSettledSuspendedNodes repairs suspension metadata from settled instance views: a parked
// VM's Node gets it, and a running VM's Node loses it. VMs the provider is changing are skipped.
func (s *ScaleSet) reconcileSettledSuspendedNodes(
	ctx context.Context, vms []*armcompute.VirtualMachineScaleSetVM, nodeList *apiv1.NodeList,
) error {
	byProviderID := make(map[string]*armcompute.VirtualMachineScaleSetVM, len(vms))
	for _, vm := range vms {
		byProviderID[normalizeProviderID(azurePrefix+*vm.ID)] = vm
	}

	var errs []error
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		vm := byProviderID[normalizeProviderID(node.Spec.ProviderID)]
		if vm == nil {
			continue
		}
		// Skip VMs with a provider operation or override; their own reconcile handles them.
		s.powerMutex.Lock()
		_, pending := s.pendingPower[*vm.Properties.VMID]
		_, overridden := s.powerOverrides[*vm.Properties.VMID]
		s.powerMutex.Unlock()
		if pending || overridden {
			continue
		}

		if ptr.Deref(vm.Properties.ProvisioningState, "") != provisioningStateSucceeded {
			continue
		}

		// A parked VM's Node needs complete metadata; a running VM's Node must have none.
		parked := isAuthoritativelyParked(vm)
		if parked && hasSuspendedMetadata(node) {
			continue
		}

		if !parked {
			if suspendedPowerState(vm) != vmPowerStateRunning {
				continue
			}
			suspended := taints.HasTaint(node, suspendedTaintKey) ||
				slices.ContainsFunc(node.Status.Conditions, func(condition apiv1.NodeCondition) bool {
					return condition.Type == suspendedCondition && condition.Status == apiv1.ConditionTrue
				})
			if !suspended {
				continue
			}
		}

		if err := s.setNodeSuspended(ctx, node, parked); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func hasSuspendedMetadata(node *apiv1.Node) bool {
	if node.Annotations[scaleDownDisabledAnnotation] != "true" {
		return false
	}

	index := slices.IndexFunc(node.Status.Conditions, func(condition apiv1.NodeCondition) bool {
		return condition.Type == suspendedCondition
	})
	if index < 0 || node.Status.Conditions[index].Status != apiv1.ConditionTrue {
		return false
	}

	suspendedTaints := 0
	for _, taint := range node.Spec.Taints {
		switch taint.Key {
		case taints.ToBeDeletedTaint, taints.DeletionCandidateTaintKey:
			return false
		case suspendedTaintKey:
			if taint != (apiv1.Taint{Key: suspendedTaintKey, Effect: apiv1.TaintEffectNoSchedule}) {
				return false
			}
			suspendedTaints++
		}
	}
	return suspendedTaints == 1
}

func sameSuspendedNode(current, expected *apiv1.Node) error {
	if expected.UID == "" || current.UID != expected.UID ||
		normalizeProviderID(current.Spec.ProviderID) != normalizeProviderID(expected.Spec.ProviderID) {
		return fmt.Errorf("node %s %w", expected.Name, errNodeChangedIdentity)
	}
	return nil
}

// nodeGone reports whether a retained Node was removed or replaced.
func nodeGone(err error) bool {
	return apierrors.IsNotFound(err) || errors.Is(err, errNodeChangedIdentity)
}

// setNodeSuspended sets or clears a Node's Suspended condition, suspended taint and scale-down-disabled
// annotation. Suspending also removes core's deletion taints and any cordon core added.
func (s *ScaleSet) setNodeSuspended(ctx context.Context, node *apiv1.Node, suspended bool) error {
	client := s.manager.kubeClient.CoreV1().Nodes()
	status, reason := apiv1.ConditionFalse, resumedReason
	if suspended {
		status, reason = apiv1.ConditionTrue, parkedReason
	}

	// Update the condition through the status subresource.
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if err := sameSuspendedNode(current, node); err != nil {
			return err
		}
		current = current.DeepCopy()
		if !setSuspendedCondition(current, status, reason, metav1.Now()) {
			return nil
		}

		_, err = client.UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("set %s=%s on node %s: %w", suspendedCondition, status, node.Name, err)
	}

	// Update taints, annotations and cordon on the Node object.
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if err := sameSuspendedNode(current, node); err != nil {
			return err
		}

		updated := current.DeepCopy()
		coreCordon := taints.HasToBeDeletedTaint(current) && s.manager.cordonNodeBeforeTerminate
		kept := make([]apiv1.Taint, 0, len(updated.Spec.Taints)+1)
		for _, taint := range updated.Spec.Taints {
			coreDeletion := taint.Key == taints.ToBeDeletedTaint || taint.Key == taints.DeletionCandidateTaintKey
			if taint.Key == suspendedTaintKey || suspended && coreDeletion {
				continue
			}
			kept = append(kept, taint)
		}
		if suspended {
			if updated.Annotations[scaleDownDisabledAnnotation] != "true" {
				metav1.SetMetaDataAnnotation(&updated.ObjectMeta, scaleDownDisabledOwnerAnnotation, "true")
			}
			metav1.SetMetaDataAnnotation(&updated.ObjectMeta, scaleDownDisabledAnnotation, "true")
			kept = append(kept, apiv1.Taint{Key: suspendedTaintKey, Effect: apiv1.TaintEffectNoSchedule})
			if coreCordon {
				updated.Spec.Unschedulable = false
			}
		} else {
			if updated.Annotations[scaleDownDisabledOwnerAnnotation] == "true" {
				delete(updated.Annotations, scaleDownDisabledAnnotation)
			}
			delete(updated.Annotations, scaleDownDisabledOwnerAnnotation)
		}

		updated.Spec.Taints = kept
		if equality.Semantic.DeepEqual(current, updated) {
			return nil
		}

		_, err = client.Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("update suspended metadata on node %s: %w", node.Name, err)
	}

	return nil
}

func setSuspendedCondition(node *apiv1.Node, status apiv1.ConditionStatus, reason string, now metav1.Time) bool {
	condition := apiv1.NodeCondition{
		Type: suspendedCondition, Status: status, Reason: reason,
		Message:           "Set by the Azure cluster autoscaler provider",
		LastHeartbeatTime: now, LastTransitionTime: now,
	}

	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type != suspendedCondition {
			continue
		}

		if node.Status.Conditions[i].Status == status {
			return false
		}

		node.Status.Conditions[i] = condition
		return true
	}

	node.Status.Conditions = append(node.Status.Conditions, condition)
	return true
}
