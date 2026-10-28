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
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v8"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/policy/retryrepectthrottled"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
)

type vmssPowerClient interface {
	BeginDeallocate(context.Context, string, string, string,
		*armcompute.VirtualMachineScaleSetVMsClientBeginDeallocateOptions,
	) (*runtime.Poller[armcompute.VirtualMachineScaleSetVMsClientDeallocateResponse], error)
	BeginStart(context.Context, string, string, string,
		*armcompute.VirtualMachineScaleSetVMsClientBeginStartOptions,
	) (*runtime.Poller[armcompute.VirtualMachineScaleSetVMsClientStartResponse], error)
}

type suspendedPowerOperation struct {
	sequence  uint64
	node      *apiv1.Node
	park      bool
	polling   bool
	completed bool
	failed    bool
	// resumed is the Start time from the Node's resume marker. A park keeps it to re-park a failed resume.
	resumed time.Time
}

type suspendedVMSSState struct {
	parkMutex      sync.Mutex
	powerMutex     sync.Mutex
	inventoryMutex sync.Mutex
	// Pending operations and overrides are keyed by VMID, not the reusable instance ID.
	pendingPower           map[string]suspendedPowerOperation
	powerSequence          uint64
	powerOverrides         map[string]bool
	parkedVMIDs            map[string]bool
	suspendedVMs           []*armcompute.VirtualMachineScaleSetVM
	suspendedInventoryTime time.Time
	suspendedInventorySize int64
	suspendedReconcileTime time.Time
	aksPoolCheckTime       time.Time
	aksPoolCheckError      error
}

type unsupportedDeallocateError struct {
	name   string
	reason error
}

func (e *unsupportedDeallocateError) Error() string {
	return fmt.Sprintf("node group %q cannot use deallocate mode: %v", e.name, e.reason)
}

func normalizeProviderID(id string) string {
	return strings.ToLower(strings.TrimSpace(id))
}

func (s *ScaleSet) suspendsParkedNodes() bool {
	return s.deallocate
}

// validateSuspendedMode checks that the group can park VMs: Uniform VMSS with Regular or Spot priority
// and managed, non-ephemeral OS disks, plus the pool's policy for AKS pools.
func (s *ScaleSet) validateSuspendedMode() error {
	cfg := s.manager.config
	hosted := cfg.HostedSubscriptionID != "" || cfg.HostedResourceGroup != "" || cfg.HostedResourceProxyURL != ""

	vmss, err := s.getVMSSFromCache()
	if err != nil {
		return err
	}

	p := vmss.Properties
	if p == nil || p.OrchestrationMode == nil || *p.OrchestrationMode != armcompute.OrchestrationModeUniform {
		return fmt.Errorf("deallocate mode requires Uniform VMSS %s", s.Name)
	}
	if p.VirtualMachineProfile == nil {
		return fmt.Errorf("missing VM profile for %s", s.Name)
	}

	priority := p.VirtualMachineProfile.Priority
	supportedPriority := priority == nil || *priority == armcompute.VirtualMachinePriorityTypesRegular ||
		*priority == armcompute.VirtualMachinePriorityTypesSpot
	if !supportedPriority {
		return fmt.Errorf("deallocate mode requires Regular or Spot priority VMSS %s", s.Name)
	}

	for tag := range vmss.Tags {
		if strings.HasPrefix(strings.ToLower(tag), "aks-managed-") {
			if err := s.validateAKSSuspendedMode(ptr.Deref(vmss.Tags[agentpoolNameTag], ""), hosted); err != nil {
				return err
			}
			break
		}
	}

	disks := p.VirtualMachineProfile.StorageProfile
	if disks == nil || disks.OSDisk == nil || disks.OSDisk.ManagedDisk == nil || disks.OSDisk.DiffDiskSettings != nil {
		return fmt.Errorf("deallocate mode requires managed non-ephemeral OS disks for %s", s.Name)
	}

	return nil
}

// validateAKSSuspendedMode checks that an AKS pool may be parked. Hosted AKS needs an explicit
// Deallocate policy; otherwise the agent pool must use Deallocate scale-down mode.
func (s *ScaleSet) validateAKSSuspendedMode(poolName string, hosted bool) error {
	if hosted {
		if !s.explicitDeallocate {
			return fmt.Errorf("hosted deallocate mode requires an explicit Deallocate policy for AKS VMSS %s", s.Name)
		}
		return nil
	}

	cfg := s.manager.config
	if poolName == "" || cfg.ClusterName == "" || cfg.ClusterResourceGroup == "" {
		return fmt.Errorf("deallocate mode requires an AKS pool name, clusterName and clusterResourceGroup for %s", s.Name)
	}

	client := s.manager.azClient.agentPoolClient
	if client == nil {
		return fmt.Errorf("deallocate mode requires an AKS agent pool client for %s", s.Name)
	}

	// Read the agent pool at most once per cache refresh interval.
	s.inventoryMutex.Lock()
	defer s.inventoryMutex.Unlock()
	if s.aksPoolCheckTime.Add(s.manager.azureCache.refreshInterval).After(time.Now()) {
		return s.aksPoolCheckError
	}

	ctx, cancel := getContextWithTimeout(vmssContextTimeout)
	defer cancel()
	pool, err := client.Get(ctx, cfg.ClusterResourceGroup, cfg.ClusterName, poolName, nil)
	if err != nil {
		err = fmt.Errorf("read AKS agent pool %s for VMSS %s: %w", poolName, s.Name, err)
	} else if pool.Properties == nil || ptr.Deref(pool.Properties.ScaleDownMode, "") != armcontainerservice.ScaleDownModeDeallocate {
		err = fmt.Errorf("deallocate mode requires AKS agent pool %s to use Deallocate scale-down mode", poolName)
	}

	s.aksPoolCheckTime, s.aksPoolCheckError = time.Now(), err
	return err
}

func suspendedPowerState(vm *armcompute.VirtualMachineScaleSetVM) string {
	if vm == nil || vm.Properties == nil || vm.Properties.InstanceView == nil {
		return vmPowerStateUnknown
	}
	return vmPowerStateFromStatuses(vm.Properties.InstanceView.Statuses)
}

func isAuthoritativelyParked(vm *armcompute.VirtualMachineScaleSetVM) bool {
	return vm != nil && vm.Properties != nil && suspendedPowerState(vm) == vmPowerStateDeallocated &&
		ptr.Deref(vm.Properties.ProvisioningState, "") == provisioningStateSucceeded
}

func (s *ScaleSet) lockParkOperation(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.parkMutex.TryLock() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// suspendedInventory returns the scale set's VMs with their instance views and records which
// of them count as parked. It reads Azure when refresh is set, the cache has expired or the VM
// count mismatches a changed capacity, and returns an error rather than stale data if that read fails.
func (s *ScaleSet) suspendedInventory(ctx context.Context, refresh bool) ([]*armcompute.VirtualMachineScaleSetVM, error) {
	if err := s.validateSuspendedMode(); err != nil {
		return nil, err
	}

	size, err := s.getScaleSetSize()
	if err != nil {
		return nil, err
	}

	// Reread the VMs when the cached list may be out of date.
	s.inventoryMutex.Lock()
	defer s.inventoryMutex.Unlock()
	stale := !s.suspendedInventoryTime.Add(s.instancesRefreshPeriod).After(time.Now())
	if refresh || stale || int64(len(s.suspendedVMs)) != size && size != s.suspendedInventorySize {
		readCtx, cancel := context.WithTimeout(ctx, vmssContextTimeout)
		defer cancel()
		vms, err := s.manager.azClient.virtualMachineScaleSetVMsClient.ListVMInstanceView(
			readCtx, s.manager.config.ResourceGroup, s.Name)
		if err != nil {
			return nil, fmt.Errorf("read VM inventory for VMSS %s: %w", s.Name, err)
		}

		s.suspendedVMs = vms
		s.suspendedInventoryTime = time.Now()
		s.suspendedInventorySize = size
	}

	// Classify each VM using Azure's view and the provider's pending operations and overrides.
	s.powerMutex.Lock()
	defer s.powerMutex.Unlock()
	observed := make(map[string]bool, len(s.suspendedVMs))
	parked := make(map[string]bool, len(s.suspendedVMs))
	for _, vm := range s.suspendedVMs {
		if vm == nil || vm.ID == nil || vm.InstanceID == nil || vm.Properties == nil {
			return nil, fmt.Errorf("incomplete VM instance view for %s", s.Name)
		}
		vmID := ptr.Deref(vm.Properties.VMID, "")
		if vmID == "" || *vm.ID == "" {
			return nil, fmt.Errorf("missing VM identity for %s", s.Name)
		}

		observed[vmID] = true
		op, pending := s.pendingPower[vmID]
		desiredParked, hasOverride := s.powerOverrides[vmID]
		provisioning := ptr.Deref(vm.Properties.ProvisioningState, "")
		powerState := suspendedPowerState(vm)

		resumeFailed := pending && !op.park && provisioning == VMProvisioningStateFailed
		if resumeFailed {
			op.failed = true
			s.pendingPower[vmID] = op
		}

		inFlight := provisioning == VMProvisioningStateCreating || provisioning == VMProvisioningStateDeleting
		unexplainedPowerState := powerState == vmPowerStateUnknown && !pending && !hasOverride &&
			!inFlight && provisioning != VMProvisioningStateFailed
		if unexplainedPowerState {
			return nil, fmt.Errorf("unknown VM power state for %s", *vm.ID)
		}

		// An override holds an accepted Start or Deallocate until Azure's view shows it.
		isParked := isAuthoritativelyParked(vm)
		if hasOverride {
			caughtUp := (desiredParked && isParked) || (!desiredParked && powerState == vmPowerStateRunning)
			if caughtUp && !pending {
				delete(s.powerOverrides, vmID)
			}
			isParked = desiredParked
		}
		if isParked {
			parked[vmID] = true
		}
	}

	// Forget VMs that left the scale set, such as deleted or evicted ones.
	maps.DeleteFunc(s.powerOverrides, func(vmID string, _ bool) bool { return !observed[vmID] })
	maps.DeleteFunc(s.pendingPower, func(vmID string, _ suspendedPowerOperation) bool { return !observed[vmID] })

	s.parkedVMIDs = parked
	return s.suspendedVMs, nil
}

// parkedCount returns the parked VMs from the last inventory read, so MinSize and MaxSize never wait on Azure.
func (s *ScaleSet) parkedCount() int {
	s.powerMutex.Lock()
	defer s.powerMutex.Unlock()
	return len(s.parkedVMIDs)
}

func (s *ScaleSet) hasResumes() bool {
	s.powerMutex.Lock()
	defer s.powerMutex.Unlock()
	for _, op := range s.pendingPower {
		if !op.park {
			return true
		}
	}
	return false
}

// forgetPower stops tracking a VM's power operation and falls back to its instance view.
func (s *ScaleSet) forgetPower(vm *armcompute.VirtualMachineScaleSetVM) {
	vmID := *vm.Properties.VMID
	s.powerMutex.Lock()
	defer s.powerMutex.Unlock()
	delete(s.pendingPower, vmID)
	delete(s.powerOverrides, vmID)
	if isAuthoritativelyParked(vm) {
		s.parkedVMIDs[vmID] = true
	} else {
		delete(s.parkedVMIDs, vmID)
	}
}

func (s *ScaleSet) recordPower(vmID string, operation suspendedPowerOperation) uint64 {
	s.powerMutex.Lock()
	defer s.powerMutex.Unlock()
	if s.pendingPower == nil {
		s.pendingPower = make(map[string]suspendedPowerOperation)
	}
	if s.powerOverrides == nil {
		s.powerOverrides = make(map[string]bool)
	}

	s.powerSequence++
	operation.sequence = s.powerSequence
	s.pendingPower[vmID] = operation
	s.powerOverrides[vmID] = operation.park
	if s.parkedVMIDs == nil {
		s.parkedVMIDs = make(map[string]bool)
	}

	if operation.park {
		s.parkedVMIDs[vmID] = true
	} else {
		delete(s.parkedVMIDs, vmID)
	}
	return operation.sequence
}

func (s *ScaleSet) powerOperation(vmID string) (suspendedPowerOperation, bool) {
	s.powerMutex.Lock()
	defer s.powerMutex.Unlock()
	op, ok := s.pendingPower[vmID]
	return op, ok
}

func (s *ScaleSet) finishPowerPolling(vmID string, sequence uint64, err error) {
	s.powerMutex.Lock()
	defer s.powerMutex.Unlock()
	if op, ok := s.pendingPower[vmID]; ok && op.sequence == sequence {
		op.polling, op.completed = false, err == nil
		op.failed = op.failed || isTerminalPowerOperationFailure(err)
		// A re-park stays counted as parked; its VM is suspended and is retried until parked.
		if op.park && op.failed && op.resumed.IsZero() {
			delete(s.powerOverrides, vmID)
			delete(s.parkedVMIDs, vmID)
		}
		s.pendingPower[vmID] = op
	}
}

// reconcileSuspendedPower settles pending power operations and continues past per-VM errors.
func (s *ScaleSet) reconcileSuspendedPower(ctx context.Context, vms []*armcompute.VirtualMachineScaleSetVM, nodeList *apiv1.NodeList) error {
	// Resume reads share one Node list, including outside Refresh.
	nodes := make(map[string]*apiv1.Node)
	needsResumeNodes := false
	for _, vm := range vms {
		if op, pending := s.powerOperation(*vm.Properties.VMID); pending && !op.park {
			needsResumeNodes = true
			break
		}
	}
	if needsResumeNodes {
		if nodeList == nil {
			var err error
			nodeList, err = s.manager.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				return fmt.Errorf("list nodes to reconcile resumes: %w", err)
			}
		}
		for i := range nodeList.Items {
			node := &nodeList.Items[i]
			nodes[node.Name] = node
		}
	}

	// Continue reconciling other VMs after a power operation fails.
	var errs []error
	for _, vm := range vms {
		op, pending := s.powerOperation(*vm.Properties.VMID)
		if !pending {
			continue
		}

		var err error
		if op.park {
			err = s.reconcilePark(ctx, vm, op)
		} else {
			err = s.reconcileResume(ctx, vm, op, nodes[op.node.Name])
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// reconcilePark settles a park, and keeps re-parking a failed resume until the provider's own
// Deallocate completes.
func (s *ScaleSet) reconcilePark(ctx context.Context, vm *armcompute.VirtualMachineScaleSetVM, op suspendedPowerOperation) error {
	// A Start may still run during a re-park, so only its own completed Deallocate parks the VM.
	parked := op.completed || op.resumed.IsZero() && isAuthoritativelyParked(vm)
	if !op.polling && parked {
		if err := s.setNodeSuspended(ctx, op.node, true); err != nil && !nodeGone(err) {
			return err
		}
		s.powerMutex.Lock()
		delete(s.pendingPower, *vm.Properties.VMID)
		s.powerMutex.Unlock()
		return nil
	}
	// Wait while a Deallocate is polling.
	if op.polling {
		return nil
	}
	// Retire an unconfirmed or failed park only once provisioning settles.
	if op.resumed.IsZero() {
		state := suspendedPowerState(vm)
		provisioning := ptr.Deref(vm.Properties.ProvisioningState, "")
		if (state == vmPowerStateRunning || state == vmPowerStateStopped || state == vmPowerStateDeallocated) &&
			(provisioning == provisioningStateSucceeded || provisioning == VMProvisioningStateFailed) {
			s.forgetPower(vm)
		}
		return nil
	}

	// Only re-park a failed resume while its Node is retained.
	current, err := s.manager.kubeClient.CoreV1().Nodes().Get(ctx, op.node.Name, metav1.GetOptions{})
	if err == nil {
		err = sameSuspendedNode(current, op.node)
	}
	vmID := *vm.Properties.VMID
	if err == nil {
		// Record the re-park before any Deallocate, so a restart does not resume the VM.
		if marker, _ := nodeResumeMarker(current); !marker.Repark {
			err = s.setResumeMarker(ctx, op.node, &resumeMarker{
				VMID: vmID, NodeUID: op.node.UID, Started: op.resumed, Repark: true,
			})
		}
	}
	if nodeGone(err) {
		s.forgetPower(vm)
		return nil
	} else if err != nil {
		return err
	}

	// Deallocate is safe to repeat. Azure may accept or reject it while a Start runs; a later loop retries a rejection.
	submitCtx, cancel := context.WithTimeout(ctx, vmssContextTimeout)
	defer cancel()
	poller, err := s.manager.azClient.vmssPowerClient.BeginDeallocate(
		submitCtx, s.manager.config.ResourceGroup, s.Name, *vm.InstanceID, nil)
	if err != nil {
		return fmt.Errorf("re-park node %s: %w", op.node.Name, err)
	}
	if poller == nil {
		return fmt.Errorf("re-park node %s has uncertain acceptance: no operation returned", op.node.Name)
	}

	// Track the new Deallocate and poll it in the background.
	sequence := s.recordPower(vmID, suspendedPowerOperation{
		node: op.node, park: true, polling: true, resumed: op.resumed,
	})
	go waitForSuspendedPower(s, poller, vmID, *vm.ID, sequence)
	return nil
}

// reconcileResume unsuspends a resumed Node once it is Ready, or re-parks it and backs off
// the group when the Start fails or the resume outlives MaxNodeProvisionTime.
func (s *ScaleSet) reconcileResume(ctx context.Context, vm *armcompute.VirtualMachineScaleSetVM, op suspendedPowerOperation, current *apiv1.Node) error {
	// Stop tracking if the Node was deleted or replaced, or its marker was cleared.
	if current == nil || nodeGone(sameSuspendedNode(current, op.node)) || current.Annotations[resumeMarkerAnnotation] == "" {
		s.forgetPower(vm)
		return nil
	}

	// Unsuspend once Azure reports Running with provisioning Succeeded and the Node is Ready.
	running := suspendedPowerState(vm) == vmPowerStateRunning &&
		ptr.Deref(vm.Properties.ProvisioningState, "") == provisioningStateSucceeded
	if !op.failed && running {
		if readiness, err := kubernetes.GetNodeReadiness(current); err == nil && readiness.Ready {
			// The resume is forgotten once a later read no longer finds the marker.
			return s.setNodeSuspended(ctx, current, false)
		}
	}
	// Otherwise wait until MaxNodeProvisionTime from the marker.
	timeout := s.resumeTimeout(ctx)
	if !op.failed && time.Now().Before(op.resumed.Add(timeout)) {
		return nil
	}

	// The resume failed: queue one failure for backoff and re-park the VM.
	errorInfo := cloudprovider.InstanceErrorInfo{
		ErrorClass: cloudprovider.OtherErrorClass, ErrorCode: "start-deallocated-timeout",
		ErrorMessage: fmt.Sprintf("Deallocated VM did not become ready within %v", timeout),
	}
	if op.failed && time.Now().Before(op.resumed.Add(timeout)) {
		errorInfo = cloudprovider.InstanceErrorInfo{
			ErrorClass: cloudprovider.OutOfResourcesErrorClass, ErrorCode: "start-deallocated-failed",
			ErrorMessage: "Failed to start deallocated VM",
		}
	}
	klog.Warningf("Resume of node %s failed: %s, re-parking it", current.Name, errorInfo.ErrorMessage)
	op = suspendedPowerOperation{node: op.node, park: true, resumed: op.resumed}
	s.recordPower(*vm.Properties.VMID, op)
	s.manager.queueResumeFailure(s, errorInfo)
	return s.reconcilePark(ctx, vm, op)
}

func (s *ScaleSet) resumeTimeout(ctx context.Context) time.Duration {
	// Deallocate groups always return options.
	options, _ := s.GetOptions(ctx, s.manager.nodeGroupDefaults)
	return options.MaxNodeProvisionTime
}

// reconcileSuspendedNodes runs on every Refresh and reconciles each Deallocate group. The groups
// share one Node list, read only if a group needs it.
func (m *AzureManager) reconcileSuspendedNodes() error {
	ctx, cancel := context.WithTimeout(context.Background(), vmssContextTimeout)
	defer cancel()

	var nodeList *apiv1.NodeList
	listNodes := func() (*apiv1.NodeList, error) {
		if nodeList != nil {
			return nodeList, nil
		}

		if m.kubeClient == nil {
			return nil, fmt.Errorf("deallocate mode requires a Kubernetes client")
		}
		var err error
		nodeList, err = m.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, fmt.Errorf("list nodes to reconcile suspended metadata: %w", err)
		}
		return nodeList, nil
	}

	for _, group := range m.getNodeGroups() {
		s, ok := group.(*ScaleSet)
		if !ok || !s.suspendsParkedNodes() {
			continue
		}

		if err := s.reconcileSuspendedNodes(ctx, listNodes); err != nil {
			klog.Warningf("Failed to reconcile suspended nodes in %s: %v", s.Name, err)
		}
	}
	return nil
}

// reconcileSuspendedNodes rebuilds resumes from markers and repairs suspension metadata at most once
// per instance refresh period, then settles the group's pending power operations.
func (s *ScaleSet) reconcileSuspendedNodes(ctx context.Context, listNodes func() (*apiv1.NodeList, error)) error {
	if !s.parkMutex.TryLock() {
		return nil
	}
	defer s.parkMutex.Unlock()
	// Resumes wait for running VMs, so keep their instance view fresh.
	vms, err := s.suspendedInventory(ctx, s.hasResumes())
	if err != nil {
		return err
	}

	// Share the Node list between resume reconciliation and metadata repair.
	repairMetadata := !s.suspendedReconcileTime.Add(s.instancesRefreshPeriod).After(time.Now())
	var nodeList *apiv1.NodeList
	if repairMetadata || s.hasResumes() {
		nodeList, err = listNodes()
		if err != nil {
			return err
		}
	}
	// Rebuild resumes before repairing metadata, so a resuming Node isn't mistaken for a stale one.
	if repairMetadata {
		s.trackResumes(ctx, vms, nodeList)
		// Retry failed Node updates on the next loop without blocking other Nodes or groups.
		if err := s.reconcileSettledSuspendedNodes(ctx, vms, nodeList); err != nil {
			klog.Warningf("Failed to reconcile suspended metadata in %s: %v", s.Name, err)
		} else {
			s.suspendedReconcileTime = time.Now()
		}
	}

	// Settle pending Starts and Deallocates on every loop.
	if err := s.reconcileSuspendedPower(ctx, vms, nodeList); err != nil {
		klog.Warningf("Failed to reconcile power operations in %s: %v", s.Name, err)
	}
	return nil
}

func (s *ScaleSet) effectivelyParked(vm *armcompute.VirtualMachineScaleSetVM) bool {
	s.powerMutex.Lock()
	defer s.powerMutex.Unlock()
	if override, ok := s.powerOverrides[ptr.Deref(vm.Properties.VMID, "")]; ok {
		return override
	}
	return isAuthoritativelyParked(vm)
}

// suspendedNodes lists every VM in the group, parked ones included, for core.
func (s *ScaleSet) suspendedNodes(ctx context.Context) ([]cloudprovider.Instance, error) {
	if _, err := s.getVMSSFromCache(); err != nil {
		klog.Warningf("Failed to get VMSS %s: %v", s.Name, err)
		return []cloudprovider.Instance{}, nil
	}
	vms, err := s.suspendedInventory(ctx, false)
	if err != nil {
		if throttled, _ := isAzureRequestsThrottled(err); !throttled && !errors.Is(err, retryrepectthrottled.ErrTooManyRequest) {
			return nil, err
		}
		klog.Warningf("Inventory for VMSS %s is throttled, returning cached instances: %v", s.Name, err)
		s.inventoryMutex.Lock()
		vms = s.suspendedVMs
		s.inventoryMutex.Unlock()
	}

	instances := make([]cloudprovider.Instance, 0, len(vms))
	s.instanceMutex.Lock()
	defer s.instanceMutex.Unlock()
	// Keep the stock physical-deletion overlay; parking never writes it.
	deleting := make(map[string]bool)
	if s.lastInstanceRefresh.Add(s.instancesRefreshPeriod).After(time.Now()) {
		for _, instance := range s.instanceCache {
			if instance.Status != nil && instance.Status.State == cloudprovider.InstanceDeleting {
				deleting[normalizeProviderID(instance.Id)] = true
			}
		}
	}

	for _, vm := range vms {
		resourceID, err := convertResourceGroupNameToLower(*vm.ID)
		if err != nil {
			return nil, err
		}

		instance := cloudprovider.Instance{Id: azurePrefix + resourceID, Status: s.suspendedInstanceStatus(vm)}
		if deleting[normalizeProviderID(instance.Id)] {
			instance.Status = &cloudprovider.InstanceStatus{State: cloudprovider.InstanceDeleting}
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

// suspendedInstanceStatus reports a VM's state to core. Parked VMs never carry ErrorInfo,
// so core never treats them as failed creations.
func (s *ScaleSet) suspendedInstanceStatus(vm *armcompute.VirtualMachineScaleSetVM) *cloudprovider.InstanceStatus {
	status := &cloudprovider.InstanceStatus{State: cloudprovider.InstanceCreating}
	if vm == nil || vm.Properties == nil {
		return status
	}

	// A VM with an operation in flight is Creating until a park completes.
	if op, pending := s.powerOperation(ptr.Deref(vm.Properties.VMID, "")); pending {
		if op.park && op.completed {
			status.State = cloudprovider.InstanceRunning
		}
		return status
	}

	if s.effectivelyParked(vm) {
		status.State = cloudprovider.InstanceRunning
		return status
	}

	// Otherwise use provisioning and power state; a VM that isn't running yet stays Creating.
	state := suspendedPowerState(vm)
	switch ptr.Deref(vm.Properties.ProvisioningState, "") {
	case VMProvisioningStateDeleting:
		status.State = cloudprovider.InstanceDeleting
	case VMProvisioningStateCreating:
		return status
	case VMProvisioningStateFailed:
		if state == vmPowerStateRunning {
			status.State = cloudprovider.InstanceRunning
		} else if s.enableFastDeleteOnFailedProvisioning &&
			(state == vmPowerStateStopped || state == vmPowerStateDeallocated) {
			status.ErrorInfo = &cloudprovider.InstanceErrorInfo{
				ErrorClass: cloudprovider.OutOfResourcesErrorClass, ErrorCode: "provisioning-state-failed",
				ErrorMessage: "Azure failed to provision a node for this node group",
			}
		}
	default:
		if state == vmPowerStateRunning {
			status.State = cloudprovider.InstanceRunning
		}
	}

	return status
}

// Timeouts and conflicts can hide acceptance; only definite rejections allow retry.
func isDefinitePowerRejection(err error) bool {
	if errors.Is(err, retryrepectthrottled.ErrTooManyRequest) {
		return true
	}

	responseError, ok := errors.AsType[*azcore.ResponseError](err)
	if !ok {
		return false
	}

	code := responseError.StatusCode
	return code >= http.StatusBadRequest && code < http.StatusInternalServerError &&
		code != http.StatusRequestTimeout && code != http.StatusConflict
}

func isTerminalPowerOperationFailure(err error) bool {
	// The SDK reports a failed asynchronous operation as a 2xx ResponseError.
	responseError, ok := errors.AsType[*azcore.ResponseError](err)
	return ok &&
		responseError.StatusCode >= http.StatusOK && responseError.StatusCode < http.StatusMultipleChoices
}

func waitForSuspendedPower[T any](s *ScaleSet, poller *runtime.Poller[T], vmID, resourceID string, sequence uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), asyncContextTimeout)
	defer cancel()
	_, err := poller.PollUntilDone(ctx, nil)
	s.finishPowerPolling(vmID, sequence, err)
	s.invalidateSuspendedInventory()
	if err != nil {
		klog.Errorf("Power operation on %s failed: %v", resourceID, err)
	}
}

func (s *ScaleSet) invalidateSuspendedInventory() {
	s.inventoryMutex.Lock()
	defer s.inventoryMutex.Unlock()
	s.suspendedInventoryTime = time.Time{}
}
