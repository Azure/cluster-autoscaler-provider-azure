# Deallocate mode

Deallocate mode is opt-in and keeps the Kubernetes Node when a VM is parked. The provider owns VMSS Deallocate and Start operations. The core selects and drains nodes, but does not stop or start VMs.

Correct resume accounting requires the proposed core resume patch, included in [`willie-yao/cluster-autoscaler@7e24d48f`](https://github.com/willie-yao/cluster-autoscaler/commit/7e24d48f0731058aeef825331884897c56c2c17c) on `core-changes-combined`. The released core dependency remains unchanged. See [the local patched-core check](testing.md#patched-core-resume-check) before enabling deallocate mode.

## Enable

Set `deallocate: true` in the Azure configuration, or `AZURE_DEALLOCATE=true`, to enable the mode globally where supported. To select one pool instead, leave the global setting disabled and use `--nodes=<min>:<max>:Deallocate:<name>`. The fifth field for labels and taints is supported, as is `scaleDownPolicy: Deallocate` in the mounted [AKS settings file](architecture.md#aks-inputs). Ordinary three-field specs and Delete specs remain available.

All supported setups require Uniform VMSS with Regular or Spot priority and managed, non-ephemeral OS disks:

| Setup | Policy check |
| --- | --- |
| Self-managed VMSS, non-hosted | The global setting or an explicit Deallocate group enables deallocate mode. |
| Self-hosted on AKS pools | Set `clusterName` and `clusterResourceGroup` to the AKS cluster, not its node resource group. The identity needs `Microsoft.ContainerService/managedClusters/agentPools/read`. The provider reads the pool identified by the `aks-managed-poolName` VMSS tag and requires AKS `scaleDownMode: Deallocate`, rechecked once per cache refresh interval. Missing configuration, Delete or unset mode, and API errors reject the group. |
| Hosted by AKS | When `hostedSubscriptionID`, `hostedResourceGroup` or `hostedResourceProxyURL` is set, an AKS-managed VMSS requires an explicit Deallocate policy from `--nodes` or the settings file. AKS supplies the pool policy, so no agent pool API read is made. Delete or unset input cannot enable deallocate mode, even with the global setting. Power operations use the same hosted proxy endpoint and subscription as the other VMSS clients. |

Flex, standard and VMs pools are excluded. AKS VMs pools requesting Deallocate stay excluded because the Delete path cannot safely replace that policy; they need a separate design. An unsupported explicit Deallocate pool is logged and excluded from autodiscovery, not changed to Delete. Do not run another autoscaler against the same pools.

The Kubernetes identity needs `update` on `nodes/status` as well as the existing Node permissions. Add this ClusterRole rule:

```yaml
- apiGroups: [""]
  resources: ["nodes/status"]
  verbs: ["update"]
```

Pass both startup taints, so cloud-controller taints during resume do not block scheduling simulation:

```text
--startup-taint=node.cloudprovider.kubernetes.io/shutdown
--startup-taint=node.kubernetes.io/out-of-service
```

See the [chart example](../charts/cluster-autoscaler/README.md#suspended-nodes).

AKS node image upgrades and switching a pool to Delete mode delete parked VMs. Autoscaling can replace the lost capacity, but the warm VMs are lost. Keep the provider's input policy aligned with the AKS pool mode.

## Lifecycle and size contract

After deallocation completes, the provider sets `Suspended=True`, adds a `status-taint.cluster-autoscaler.kubernetes.io/azure-suspended:NoSchedule` taint and the scale-down-disabled annotation. It removes the core's deletion taints and applicable cordon, while retaining Node identity and PodCIDR.

Refresh also repairs suspension metadata from settled Azure instance views, including parks completed before a restart or by an earlier provider. It clears stale suspension metadata on settled running VMs, but skips in-flight power operations and unsettled provisioning states.

Spot VMs evicted with the Deallocate policy count and resume like parked VMs once Azure reports settled deallocation, and failed Starts use the same fallback or backoff and re-park path. VMs evicted with the Delete policy leave the inventory when refreshed and cannot be resumed.

IncreaseSize resumes parked VMs before creating new ones. Resume sets `Suspended=False` with a transition time and removes the provider's suspension metadata. The core patch counts an old, unready resumed Node as starting during its startup window.

If Azure definitely rejects a Start, the provider leaves that VM parked and tries the next parked VM once. It grows the pool by the remaining demand after trying the parked VMs, using physical capacity plus the remainder. If growth also fails, the returned error includes the Start rejections. Timeouts, conflicts and missing pollers still stop the request because Azure may have accepted the Start.

When Azure reports a terminal Start failure or a failed Start in the instance view, the provider reports the tracked resume as Creating with ErrorInfo. The core records a failed scale-up and backs off. Create-error cleanup uses the matching registered Node to park the VM again and restore suspension metadata. The VM stays charged as running capacity until parking starts. A successful Start whose Node never becomes Ready is parked again by the core's ordinary scale-down of unready nodes. That uses `--scale-down-unready-time` and doesn't wait for the startup window.

For Deallocate pools, the provider raises MaxNodeStartupTime to at least the effective MaxNodeProvisionTime plus six minutes and one second. The margin covers the two three-minute API budgets after resume and the Node timestamp's one-second precision, so the startup window ends after the request deadline. Global defaults and per-pool `maxnodeprovisiontime` and `maxnodestartuptime` tags are applied first, and a longer startup time is kept. A failed resume can then trigger the core's `ScaleUpTimedOut` and backoff while the Node still counts as starting. DecreaseTargetSize only refreshes caches and does not stop or delete resuming VMs.

TargetSize counts every VM, including parked VMs. A resume does not change TargetSize, and only newly created VMs increase it. The configured min and max apply to running VMs. With P parked VMs, MinSize returns configured min + P and MaxSize returns configured max + P.

The adjusted bounds let a pool resume after reaching its configured maximum, and stop scale-down at the configured running minimum. Minimum-size enforcement can resume parked VMs to restore the running minimum. Accepted Starts count as running capacity before Azure's instance view catches up.

Core status displays the adjusted total bounds, not the configured running bounds. Similar-group balancing still orders groups by total TargetSize. Synthetic unregistered and failed-creation cleanup physically deletes VMs that have no registered Node. Synthetic cleanup of registered instances is refused unless the provider is tracking a failed resume of the same VM and Node.

Cluster-wide limits (`--max-nodes-total`, `--cores-total` and `--memory-total`) count stopped Nodes, as in the AKS fork.
