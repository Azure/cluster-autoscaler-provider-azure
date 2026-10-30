# Deallocate mode

Deallocate mode is opt-in and keeps the Kubernetes Node when a VM is parked. The provider owns VMSS Deallocate and Start operations. The core selects and drains nodes, but does not stop or start VMs.

See [the local resume tests](testing.md#resume-accounting).

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

Pass both startup taints so core treats Nodes with lingering cloud-controller taints as NotReady and strips those taints from scheduling templates:

```text
--startup-taint=node.cloudprovider.kubernetes.io/shutdown
--startup-taint=node.kubernetes.io/out-of-service
```

The resume processor already strips these taints from in-flight resume snapshots. See the [chart example](../charts/cluster-autoscaler/README.md#deallocate-mode).

AKS node image upgrades and switching a pool to Delete mode delete parked VMs. Autoscaling can replace the lost capacity, but the warm VMs are lost. Keep the provider's input policy aligned with the AKS pool mode.

Before turning deallocate mode off for a group, delete its parked Nodes so core can remove their VMs as unregistered instances.

## Lifecycle and size contract

After deallocation completes, the provider sets `Suspended=True`, adds a `status-taint.cluster-autoscaler.kubernetes.io/azure-suspended:NoSchedule` taint and the scale-down-disabled annotation. It records ownership of that annotation with `cluster-autoscaler.kubernetes.io/azure-owns-scale-down-disabled`, and removes it on unsuspend only when provider-owned. An operator's existing `scale-down-disabled: "true"` is preserved. It removes the core's deletion taints and applicable cordon, while retaining Node identity and PodCIDR. If the Deallocate fails or its result is unknown, the scale-down fails. A definite failure releases the parked count immediately. An unknown result keeps the VM counted as parked until Azure shows it settled (provisioning Succeeded or Failed) as deallocated, running or stopped. The park then completes if the VM is deallocated with provisioning Succeeded, and is dropped otherwise.

Refresh repairs suspension metadata from settled Azure instance views. A running VM whose Node is suspended without a resume marker has its metadata cleared, so core's ordinary unready handling applies.

Spot VMs evicted with the Deallocate policy count and resume like parked VMs once Azure reports settled deallocation, and failed Starts use the same re-park and backoff path. VMs evicted with the Delete policy leave the inventory when refreshed and cannot be resumed.

IncreaseSize resumes parked VMs before creating new ones. It only starts a VM that has a retained Node. Before each Start, the provider writes a `cluster-autoscaler.kubernetes.io/azure-resume` annotation with the VM ID, Node UID and start time, and removes it if Azure definitely rejects the Start. The Node keeps `Suspended=True`, the suspended taint and the scale-down-disabled annotation, which the provider sets first if metadata repair hasn't reached the Node yet, such as after a Spot eviction. Refresh unsuspends the Node and removes the marker once the VM is running and the Node is Ready. After a restart, Refresh rebuilds in-flight resumes from their markers and keeps the original start time.

If Azure definitely rejects a Start, the provider moves on to the next parked VM, trying each parked VM at most once per request. It grows the pool by the remaining demand after trying the parked VMs, using physical capacity plus the remainder. If growth also fails, the returned error includes the Start rejections. Timeouts, conflicts and missing pollers still stop the request because Azure may have accepted the Start, and the resume stays tracked.

IncreaseSize checks headroom for the full request before starting any VM. Resumes and growth are refused when the request needs headroom from an unconfirmed park. A recorded re-park does not block replacement growth. Refresh retires an unconfirmed or failed park once Azure shows the VM settled (provisioning Succeeded or Failed) as deallocated, running or stopped. DecreaseTargetSize only refreshes caches.

A resume fails when Azure reports a terminal Start failure or a failed instance view, or when the Node is not Ready within the group's MaxNodeProvisionTime from the marker. The provider then reports one failed scale-up to core before the same loop scales up, so core backs off the group, and Refresh re-parks the VM. A re-park is complete only when a Deallocate the provider issued completes, even if the instance view already shows the VM deallocated, because a Start may still be running. Azure may accept or reject a Deallocate while a Start runs. The provider retries a rejected Deallocate on each loop with a warning. Completion restores the suspension metadata and removes the marker. A VM that cannot be re-parked stays suspended and is retried. An operator can delete its VM. Before the first re-park Deallocate, the provider records the re-park in the marker. After a restart, a recorded re-park or a resume past its deadline is re-parked even if its Node is now Ready. A resume or re-park whose Node is deleted or replaced is dropped without a power operation or a Refresh failure.

Set the VMSS tag `k8s.io_cluster-autoscaler_node-template_autoscaling-options_maxnodeprovisiontime` to a Go duration such as `5m` to set the pool's MaxNodeProvisionTime, used for both resumes and new VMs by the provider and core. Deallocate groups also read the matching maxnodestartuptime tag for core's MaxNodeStartupTime.

With capacity C and P parked VMs, TargetSize returns C, MinSize returns configured min + P, and MaxSize returns configured max + P. A resuming VM is not parked, so it counts toward the running bounds. Core counts the retained `Suspended=True` Node in its Suspended bucket, so a resume adds no upcoming template and no incorrect-size repair. The provider wraps two core processors instead. Before core's pod list processors run, it replaces each in-flight resume in the scheduling snapshot with a Ready copy that keeps its pods and DRA and CSI data, without the suspended, shutdown, not-ready, unreachable and out-of-service taints. Pending pods then pack onto the real Node. The scale-down Node processor keeps resumes, including ones that fail during the loop, and re-parks out of scale-down candidates and pod destinations. The Kubernetes Node is not changed.

The adjusted bounds let a pool resume after reaching its configured maximum, and stop scale-down at the configured running minimum. Minimum-size enforcement can resume parked VMs to restore the running minimum. Accepted Starts count as running capacity before Azure's instance view catches up.

Because a resume adds no upcoming Node, core closes its scale-up request on the next loop. `IsNodeGroupScalingUp` is then false, and group status can show no scale-up activity while a resume is in flight. Core status displays the adjusted total bounds, not the configured running bounds. Similar-group balancing still orders groups by total TargetSize. Synthetic unregistered and failed-creation cleanup physically deletes VMs that have no registered Node, and refuses registered instances. Deleting a parked Node at the running minimum leaves its VM behind, so operators must delete the VM instead.

Limitations: salvo scale-up (`--salvo-scale-up`) injects template nodes after each increase and is not supported with deallocate mode. The snapshot copy uses the resources the Node reports, so a GPU, DRA or CSI resource that disappears while kubelet restarts is not packed until it returns. Informer lag can cause a one-loop accounting gap.

Cluster-wide limits (`--max-nodes-total`, `--cores-total` and `--memory-total`) count parked Nodes, as in the AKS fork.

## Safety rules

- A Node is unsuspended only when its VM is Running with provisioning Succeeded, never during a park or re-park, and a resumed Node must also be Ready.
- An in-flight resume is schedulable only in the scheduling snapshot. The API Node keeps its suspension until the resume completes, and the resume is never a scale-down candidate or destination.
- The resume marker is written before every Start, and `repark` is recorded before every re-park Deallocate. If the write fails, nothing is sent.
- Power operations act only on the Node they were issued for, matched by UID. A missing or replaced Node retires the operation.
- A failed resume is reported to core once per process, so core backs off the group.
- A re-park is complete only when the provider's own Deallocate completes. Until then the Node stays suspended and the Deallocate is retried.
- One VM's failing re-park does not block scale-down of other Nodes, or the group's replacement growth once the re-park is recorded.
- Synthetic cleanup never deletes a VM that has a registered Node.
