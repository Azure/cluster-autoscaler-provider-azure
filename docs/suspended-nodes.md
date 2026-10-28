# Deallocate mode

Deallocate mode is opt-in and keeps the Kubernetes Node when a VM is parked. The provider owns VMSS Deallocate operations. The core selects and drains nodes, but does not stop VMs.

Parked VMs are not resumed yet. Scale-up creates new VMs instead of starting parked ones.

## Enable

Set `deallocate: true` in the Azure configuration, or `AZURE_DEALLOCATE=true`, to use the mode for every node group, including groups with a Delete policy. With the global setting, a node group that cannot use Deallocate is logged and skipped instead of kept on Delete, so it is not autoscaled. To select one pool instead, leave the global setting disabled and use `--nodes=<min>:<max>:Deallocate:<name>`. The fifth field for labels and taints is supported, as is `scaleDownPolicy: Deallocate` in the mounted [AKS settings file](architecture.md#aks-inputs). Ordinary three-field specs and Delete specs remain available.

Every setup requires Uniform VMSS with Regular or Spot priority and managed, non-ephemeral OS disks:

| Setup | Policy check |
| --- | --- |
| Self-managed VMSS, non-hosted | The global setting or an explicit Deallocate group enables deallocate mode. |
| Self-hosted on AKS pools, testing only | AKS does not support running your own autoscaler against AKS-managed pools, so use this only for testing, as the E2E suite does. Set `clusterName` and `clusterResourceGroup` to the AKS cluster, not its node resource group. The identity needs `Microsoft.ContainerService/managedClusters/agentPools/read`. The provider reads the pool identified by the `aks-managed-poolName` VMSS tag and requires AKS `scaleDownMode: Deallocate`, rechecked once per cache refresh interval. Missing configuration, Delete or unset mode, and API errors reject the group. |
| Hosted by AKS | When `hostedSubscriptionID`, `hostedResourceGroup` or `hostedResourceProxyURL` is set, an AKS-managed VMSS requires an explicit Deallocate policy from `--nodes` or the settings file. AKS supplies the pool policy, so no agent pool API read is made. Delete or unset input cannot enable deallocate mode, even with the global setting. Power operations use the same hosted proxy endpoint and subscription as the other VMSS clients. |

Flex, standard and VMs pools are excluded. AKS VMs pools requesting Deallocate stay excluded because the Delete path cannot safely replace that policy. An unsupported explicit Deallocate pool is logged and excluded from auto-discovery, not changed to Delete. An explicit group in deallocate mode, including through the global `deallocate: true` setting, must have its VMSS at startup or the group stays excluded until restart. Do not run another autoscaler against the same pools.

The Kubernetes identity needs `update` on `nodes/status` as well as the existing Node permissions. Add this ClusterRole rule:

```yaml
- apiGroups: [""]
  resources: ["nodes/status"]
  verbs: ["update"]
```

AKS node image upgrades and switching a pool to Delete mode delete parked VMs. Autoscaling can replace the lost capacity, but the warm VMs are lost. Keep the provider's input policy aligned with the AKS pool mode.

Before turning deallocate mode off for a group, delete its parked Nodes so core can remove their VMs as unregistered instances.

## Lifecycle and size contract

After deallocation completes, the provider sets `Suspended=True`, adds a `status-taint.cluster-autoscaler.kubernetes.io/azure-suspended:NoSchedule` taint and the scale-down-disabled annotation. It records ownership of that annotation with `cluster-autoscaler.kubernetes.io/azure-owns-scale-down-disabled`, and removes it on unsuspend only when provider-owned. An operator's existing `scale-down-disabled: "true"` is preserved. It removes the core's deletion taints and applicable cordon, while retaining Node identity and PodCIDR. If the Deallocate fails or its result is unknown, the scale-down fails. A definite failure releases the parked count immediately. An unknown result keeps the VM counted as parked until Azure shows it settled (provisioning Succeeded or Failed) as deallocated, running or stopped. The park then completes if the VM is deallocated with provisioning Succeeded, and is dropped otherwise.

Refresh repairs suspension metadata from settled Azure instance views. A running VM whose Node is suspended has its metadata cleared, so core's ordinary unready handling applies.

Spot VMs evicted with the Deallocate policy count like parked VMs once Azure reports settled deallocation. VMs evicted with the Delete policy leave the inventory when refreshed.

IncreaseSize grows VMSS physical capacity by the requested delta. It refuses growth only when the requested capacity needs headroom from an unconfirmed park. Refresh retires an unconfirmed or failed park once Azure shows the VM settled (provisioning Succeeded or Failed) as deallocated, running or stopped. DecreaseTargetSize only refreshes caches.

With capacity C and P parked VMs, TargetSize returns C, MinSize returns configured min + P, and MaxSize returns configured max + P. Core counts the retained `Suspended=True` Node in its Suspended bucket.

The adjusted bounds let a pool grow after reaching its configured total maximum when some VMs are parked, and stop scale-down at the configured running minimum.

Core status displays the adjusted total bounds, not the configured running bounds. Similar-group balancing still orders groups by total TargetSize. Synthetic unregistered and failed-creation cleanup physically deletes VMs that have no registered Node, and refuses registered instances. Deleting a parked Node at the running minimum leaves its VM behind, so operators must delete the VM instead.

Cluster-wide limits (`--max-nodes-total`, `--cores-total` and `--memory-total`) count parked Nodes, as in the AKS fork.

## Safety rules

- A Node is unsuspended only when its VM is Running with provisioning Succeeded, never during a park.
- Power operations act only on the Node they were issued for, matched by UID. A missing or replaced Node retires the operation.
- Synthetic cleanup never deletes a VM that has a registered Node.
