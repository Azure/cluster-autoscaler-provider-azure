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
	"net/http"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	v1lister "k8s.io/client-go/listers/core/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/cluster-autoscaler/pkg/builder"
	"sigs.k8s.io/cluster-autoscaler/pkg/cloudprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/clusterstate/scaleupfailures"
	clusterstateutils "sigs.k8s.io/cluster-autoscaler/pkg/clusterstate/utils"
	"sigs.k8s.io/cluster-autoscaler/pkg/config"
	"sigs.k8s.io/cluster-autoscaler/pkg/config/dynamic"
	ca_context "sigs.k8s.io/cluster-autoscaler/pkg/context"
	"sigs.k8s.io/cluster-autoscaler/pkg/core"
	coreoptions "sigs.k8s.io/cluster-autoscaler/pkg/core/options"
	"sigs.k8s.io/cluster-autoscaler/pkg/core/podlistprocessor"
	"sigs.k8s.io/cluster-autoscaler/pkg/debuggingsnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/estimator"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander/factory"
	"sigs.k8s.io/cluster-autoscaler/pkg/expander/priority"
	ca_processors "sigs.k8s.io/cluster-autoscaler/pkg/processors"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodeinfosprovider"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/nodes"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/pods"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/scaledowncandidates"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/scaledowncandidates/emptycandidates"
	"sigs.k8s.io/cluster-autoscaler/pkg/processors/scaledowncandidates/previouscandidates"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/predicate"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot/store"
	csisnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/csi/snapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/drainability/rules"
	drasnapshot "sigs.k8s.io/cluster-autoscaler/pkg/simulator/dynamicresources/snapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/options"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/scheduling"
	caerrors "sigs.k8s.io/cluster-autoscaler/pkg/utils/errors"
	kube_util "sigs.k8s.io/cluster-autoscaler/pkg/utils/kubernetes"
	catest "sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

// resumeLoop runs the released core's StaticAutoscaler.RunOnce against a deallocate
// "pool" group and a Delete-mode "backup" group, with priority pool > backup.
type resumeLoop struct {
	client     *fake.Clientset
	manager    *AzureManager
	pool       *ScaleSet
	autoscaler *core.StaticAutoscaler
	scaleDown  *recordingScaleDownNodeProcessor
	failures   *scaleupfailures.Registry
}

func newResumeLoop(
	t *testing.T, client *fake.Clientset, pool, backup *suspendedWorld,
	configure ...func(*dynamic.NodeGroupSpec, *config.AutoscalingOptions),
) *resumeLoop {
	t.Helper()
	loop := &resumeLoop{client: client}
	loop.manager = newSuspendedManager(t, client, pool, backup)
	loop.manager.config.Deallocate = false
	poolSpec := &dynamic.NodeGroupSpec{Name: "pool", MinSize: 0, MaxSize: 3}
	autoscalingOptions := config.AutoscalingOptions{
		NodeGroupDefaults: config.NodeGroupAutoscalingOptions{
			ScaleDownUtilizationThreshold: 0.5, ScaleDownGpuUtilizationThreshold: 0.5,
			ScaleDownUnneededTime: 10 * time.Hour, ScaleDownUnreadyTime: 10 * time.Hour,
			MaxNodeProvisionTime: 15 * time.Minute, MaxNodeStartupTime: 15 * time.Minute,
		},
		EstimatorName: estimator.BinpackingEstimatorName, ExpanderNames: expander.PriorityBasedExpanderName,
		ConfigNamespace: "kube-system", ScaleDownEnabled: true, ScaleDownSimulationTimeout: 10 * time.Second,
		MaxScaleDownParallelism: 10, MaxDrainParallelism: 1, MaxNodesPerScaleUp: 1000,
		MaxNodeGroupBinpackingDuration: 10 * time.Second, PredicateParallelism: 1,
		MaxTotalUnreadyPercentage: 45, OkTotalUnreadyCount: 3, ExpendablePodsPriorityCutoff: -10,
		ScaleDownCandidatesPoolRatio: 1, ScaleDownCandidatesPoolMinCount: 100, ScaleDownNonEmptyCandidatesCount: 30,
		CordonNodeBeforeTerminate: true,
	}
	for _, f := range configure {
		f(poolSpec, &autoscalingOptions)
	}
	var err error
	loop.pool, err = newScaleSet(azureNodeGroupSpec{NodeGroupSpec: poolSpec, policy: scaleDownPolicyDeallocate},
		loop.manager, -1, false)
	require.NoError(t, err)
	backupGroup, err := NewScaleSet(&dynamic.NodeGroupSpec{Name: "backup", MinSize: 0, MaxSize: 3}, loop.manager, -1, false)
	require.NoError(t, err)
	require.False(t, backupGroup.suspendsParkedNodes())
	loop.manager.RegisterNodeGroup(loop.pool)
	loop.manager.RegisterNodeGroup(backupGroup)
	require.NoError(t, loop.manager.forceRefresh())

	limiter := cloudprovider.NewResourceLimiter(
		map[string]int64{cloudprovider.ResourceNameCores: 0, cloudprovider.ResourceNameMemory: 0},
		map[string]int64{cloudprovider.ResourceNameCores: 1000, cloudprovider.ResourceNameMemory: 1 << 50})
	provider, err := BuildAzureCloudProvider(loop.manager, limiter)
	require.NoError(t, err)

	// Mirror the builder: defaults first, then the cloud provider wraps processors.
	ctx := t.Context()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	handle, err := framework.NewHandle(ctx, informerFactory, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handle.Framework.Close()) })
	snapshot := predicate.NewPredicateSnapshot(store.NewDeltaSnapshotStore(), handle, false, 1, false, 0)
	deleteOptions := options.NewNodeDeleteOptions(autoscalingOptions)
	drainabilityRules := rules.Default(deleteOptions)
	processors := ca_processors.DefaultProcessors(autoscalingOptions)
	processors.TemplateNodeInfoProvider = nodeinfosprovider.NewDefaultTemplateNodeInfoProvider(nil, false)
	processors.PodListProcessor = podlistprocessor.NewDefaultPodListProcessor(scheduling.ScheduleAnywhere)
	previous := previouscandidates.NewPreviousCandidates()
	processors.ScaleDownCandidatesNotifier.Register(previous)
	candidates := scaledowncandidates.NewCombinedScaleDownCandidatesProcessor()
	candidates.Register(scaledowncandidates.NewScaleDownCandidatesSortingProcessor([]scaledowncandidates.CandidatesComparer{
		emptycandidates.NewEmptySortingProcessor(emptycandidates.NewNodeInfoGetter(snapshot), deleteOptions, drainabilityRules),
		previous,
	}))
	loop.scaleDown = &recordingScaleDownNodeProcessor{ScaleDownNodeProcessor: candidates, seen: map[string]bool{}}
	processors.ScaleDownNodeProcessor = loop.scaleDown
	opts := coreoptions.AutoscalerOptions{
		AutoscalingOptions: autoscalingOptions, KubeClient: client, Processors: processors,
		FrameworkHandle: handle, ClusterSnapshot: snapshot,
		ScaleUpFailuresRegistry: scaleupfailures.NewRegistry(),
		DebuggingSnapshotter:    debuggingsnapshot.NewDebuggingSnapshotter(false),
		DeleteOptions:           deleteOptions, DrainabilityRules: drainabilityRules,
	}
	loop.manager.configureAutoscaler(&opts)
	loop.failures = scaleupfailures.NewRegistry()
	processors.ScaleStateNotifier.Register(loop.failures)

	recorder := record.NewFakeRecorder(1000)
	logRecorder, err := clusterstateutils.NewStatusMapRecorder(client, "kube-system", recorder, false, "status")
	require.NoError(t, err)
	allNodes := kube_util.NewDynamicTestNodeLister(client)
	daemonSets, err := kube_util.NewTestDaemonSetLister(nil)
	require.NoError(t, err)
	kubeClients := &ca_context.AutoscalingKubeClients{
		ListerRegistry: kube_util.NewListerRegistry(allNodes, readyNodeLister{allNodes}, clientPodLister{client},
			kube_util.NewTestPodDisruptionBudgetLister(nil), daemonSets, nil, nil, nil, nil),
		ClientSet: client, Recorder: recorder, LogRecorder: logRecorder,
	}

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, indexer.Add(&apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: priority.PriorityConfigMapName, Namespace: "kube-system"},
		Data:       map[string]string{priority.ConfigMapKey: "20:\n  - ^pool$\n10:\n  - ^backup$\n"},
	}))
	expanders := factory.NewFactory()
	expanders.RegisterFilter(expander.PriorityBasedExpanderName, func() expander.Filter {
		return priority.NewFilter(v1lister.NewConfigMapLister(indexer).ConfigMaps("kube-system"), recorder)
	})
	strategy, typedErr := expanders.Build([]string{expander.PriorityBasedExpanderName})
	require.NoError(t, typedErr)

	opts.AutoscalingKubeClients, opts.ExpanderStrategy = kubeClients, strategy
	opts.CloudProvider = provider
	autoscaler, typedErr := builder.NewAutoscaler(ctx, opts, informerFactory)
	require.NoError(t, typedErr)
	loop.autoscaler = autoscaler.(*core.StaticAutoscaler)
	return loop
}

// runOnce waits one scan interval, as core requires distinct loop times, then runs a loop.
func (l *resumeLoop) runOnce(t *testing.T) {
	t.Helper()
	time.Sleep(10 * time.Second)
	synctest.Wait()
	l.scaleDown.reset()
	require.NoError(t, l.autoscaler.RunOnce(t.Context(), time.Now()))
	synctest.Wait()
}

func (l *resumeLoop) node(t *testing.T, name string) *apiv1.Node {
	t.Helper()
	return getTestNode(t, l.client, name)
}

func (l *resumeLoop) setReady(t *testing.T, name string) {
	t.Helper()
	node := l.node(t, name)
	catest.SetNodeReadyState(node, true, time.Now())
	node.Spec.Taints = slices.DeleteFunc(node.Spec.Taints, func(taint apiv1.Taint) bool {
		return taint.Key != suspendedTaintKey
	})
	_, err := l.client.CoreV1().Nodes().Update(t.Context(), node, metav1.UpdateOptions{})
	require.NoError(t, err)
	_, err = l.client.CoreV1().Nodes().UpdateStatus(t.Context(), node, metav1.UpdateOptions{})
	require.NoError(t, err)
}

// snapshotPods returns the pods the last loop's snapshot placed on a Node.
func (l *resumeLoop) snapshotPods(t *testing.T, name string) []string {
	t.Helper()
	nodeInfo, err := l.autoscaler.ClusterSnapshot.GetNodeInfo(name)
	require.NoError(t, err)
	var pods []string
	for _, pod := range nodeInfo.Pods() {
		pods = append(pods, pod.Name)
	}
	return pods
}

func (l *resumeLoop) backedOff(t *testing.T) bool {
	t.Helper()
	return l.autoscaler.ClusterStateRegistry.BackoffStatusForNodeGroup(t.Context(), l.pool, time.Now()).IsBackedOff
}

// requireSteadyResume checks that core keeps the resume as it is: no target repair,
// no template node for it and no scale-down role for its Node.
func (l *resumeLoop) requireSteadyResume(t *testing.T, name string) {
	t.Helper()
	registry := l.autoscaler.ClusterStateRegistry
	require.Nil(t, registry.GetIncorrectNodeGroupSize(l.pool.Id()))
	upcoming, _ := registry.GetUpcomingNodes(t.Context())
	require.Zero(t, upcoming[l.pool.Id()])
	require.False(t, l.scaleDown.saw(name), "an in-flight resume is never a scale-down candidate or destination")
	require.True(t, l.scaleDown.saw("node-0"), "core's scale-down processor still runs")
}

type readyNodeLister struct {
	*kube_util.DynamicTestNodeLister
}

func (l readyNodeLister) List() ([]*apiv1.Node, error) {
	allNodes, err := l.DynamicTestNodeLister.List()
	return slices.DeleteFunc(allNodes, func(node *apiv1.Node) bool {
		return !kube_util.IsNodeReadyAndSchedulable(node)
	}), err
}

type clientPodLister struct {
	client *fake.Clientset
}

func (l clientPodLister) List() ([]*apiv1.Pod, error) {
	list, err := l.client.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pods := make([]*apiv1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	return pods, nil
}

type recordingScaleDownNodeProcessor struct {
	nodes.ScaleDownNodeProcessor
	mu   sync.Mutex
	seen map[string]bool
}

func (p *recordingScaleDownNodeProcessor) record(allNodes []*apiv1.Node) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, node := range allNodes {
		p.seen[node.Name] = true
	}
}

func (p *recordingScaleDownNodeProcessor) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = map[string]bool{}
}

// saw reports whether core's scale-down processor received the Node in the last loop.
func (p *recordingScaleDownNodeProcessor) saw(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen[name]
}

func (p *recordingScaleDownNodeProcessor) GetPodDestinationCandidates(autoscalingCtx *ca_context.AutoscalingContext, allNodes []*apiv1.Node) ([]*apiv1.Node, caerrors.AutoscalerError) {
	p.record(allNodes)
	return p.ScaleDownNodeProcessor.GetPodDestinationCandidates(autoscalingCtx, allNodes)
}

func (p *recordingScaleDownNodeProcessor) GetScaleDownCandidates(ctx context.Context, autoscalingCtx *ca_context.AutoscalingContext, allNodes []*apiv1.Node) ([]*apiv1.Node, caerrors.AutoscalerError) {
	p.record(allNodes)
	return p.ScaleDownNodeProcessor.GetScaleDownCandidates(ctx, autoscalingCtx, allNodes)
}

// newResumeCluster has a full Ready pool Node, parked pool Nodes for the remaining VMs,
// an empty Delete-mode backup group and one pending pod.
func newResumeCluster(t *testing.T, pool *suspendedWorld) *fake.Clientset {
	t.Helper()
	active := suspendedPoolNode(pool, 0, false)
	full := catest.BuildTestPod("full", 3500, 1024, catest.WithNodeName(active.Name))
	pending := catest.BuildTestPod("pending", 1000, 1024, catest.MarkUnschedulable(),
		catest.WithCreationTimestamp(time.Now().Add(-time.Minute)))
	objects := []runtime.Object{active, full, pending}
	for i := 1; i < len(pool.states); i++ {
		parked := suspendedPoolNode(pool, i, true)
		catest.SetNodeReadyState(parked, false, time.Now().Add(-time.Hour))
		// Cloud and node lifecycle controllers taint a stopped VM's Node.
		parked.Spec.Taints = append(parked.Spec.Taints,
			apiv1.Taint{Key: "node.cloudprovider.kubernetes.io/shutdown", Effect: apiv1.TaintEffectNoSchedule},
			apiv1.Taint{Key: apiv1.TaintNodeUnreachable, Effect: apiv1.TaintEffectNoExecute})
		objects = append(objects, parked)
	}
	return fake.NewClientset(objects...)
}

func TestSuspendedLoopResumePacksAndCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateDeallocated, vmPowerStateDeallocated}}
		backup := &suspendedWorld{name: "backup"}
		client := newResumeCluster(t, pool)
		loop := newResumeLoop(t, client, pool, backup)

		loop.runOnce(t)
		require.Equal(t, []string{"start:1"}, pool.history(), "the priority expander picks the deallocate pool")
		resuming := loop.node(t, "node-1")
		require.True(t, hasSuspendedMetadata(resuming), "the resuming Node stays suspended")
		marker, marked := nodeResumeMarker(resuming)
		require.True(t, marked)
		require.Equal(t, "vm-1", marker.VMID)
		require.Equal(t, resuming.UID, marker.NodeUID)
		require.True(t, marker.Started.Equal(time.Now()))
		require.False(t, marker.Repark)
		target, err := loop.pool.TargetSize(t.Context())
		require.NoError(t, err)
		require.Equal(t, 3, target, "a resume does not change the target")
		require.Equal(t, 1, loop.pool.MinSize(t.Context()))
		require.Equal(t, 4, loop.pool.MaxSize(t.Context()))

		for range 3 {
			time.Sleep(time.Minute)
			loop.runOnce(t)
			require.Equal(t, []string{"start:1"}, pool.history(), "pending pods pack without another IncreaseSize")
			require.Empty(t, backup.history())
			require.Contains(t, loop.snapshotPods(t, "node-1"), "pending", "core packs the pod onto the real Node")
			loop.requireSteadyResume(t, "node-1")
		}

		loop.setReady(t, "node-1")
		loop.runOnce(t)
		require.Contains(t, loop.snapshotPods(t, "node-1"), "pending")
		resumed := loop.node(t, "node-1")
		require.Equal(t, apiv1.ConditionFalse, nodeSuspended(resumed).Status)
		require.False(t, hasSuspendedMetadata(resumed))
		require.NotContains(t, resumed.Annotations, resumeMarkerAnnotation)
		require.NotContains(t, resumed.Annotations, scaleDownDisabledAnnotation)
		loop.requireSteadyResume(t, "node-1")

		loop.runOnce(t)
		require.Empty(t, loop.manager.resumingNodes(), "the resume ends once its cleared marker is observed")
		require.Contains(t, loop.snapshotPods(t, "node-1"), "pending", "the resumed Node takes the pod as an ordinary Node")
		require.Equal(t, []string{"start:1"}, pool.history())
		require.Empty(t, backup.history())
		require.False(t, loop.backedOff(t))
		require.Empty(t, loop.failures.Get())
		require.Equal(t, 1, loop.pool.parkedCount(), "the other VM stays parked")
	})
}

func TestSuspendedLoopFailureRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, power string
		pollErr     error
		viewFailed  bool
		wait        time.Duration
		restart     bool
		ready       bool
	}{
		{name: "terminal Start failure", pollErr: &azcore.ResponseError{StatusCode: http.StatusOK}},
		{name: "failed instance view", pollErr: context.DeadlineExceeded, viewFailed: true},
		{name: "failed instance view while running", pollErr: context.DeadlineExceeded, viewFailed: true,
			power: vmPowerStateRunning},
		{name: "unready timeout", wait: 14 * time.Minute},
		{name: "poll timeout", pollErr: context.DeadlineExceeded, wait: 14 * time.Minute},
		{name: "restart running", restart: true, wait: 10 * time.Minute, power: vmPowerStateRunning},
		{name: "restart deallocated", restart: true, wait: 10 * time.Minute, power: vmPowerStateDeallocated},
		{name: "restart expired unready", restart: true, wait: 16 * time.Minute, power: vmPowerStateDeallocated},
		{name: "restart expired Ready", restart: true, wait: 16 * time.Minute, power: vmPowerStateRunning, ready: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pool := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateDeallocated}}
				if tc.pollErr != nil {
					pool.startResult = func(int) error { return tc.pollErr }
				}
				backup := &suspendedWorld{name: "backup"}
				client := newResumeCluster(t, pool)
				loop := newResumeLoop(t, client, pool, backup)
				retained := loop.node(t, "node-1")
				loop.runOnce(t)
				started := time.Now()
				require.Equal(t, []string{"start:1"}, pool.history())
				if tc.viewFailed {
					pool.setPowerState(1, vmPowerStateStopped)
					pool.provisioning = map[int]string{1: VMProvisioningStateFailed}
					loop.pool.invalidateSuspendedInventory()
				}
				if tc.ready {
					loop.setReady(t, "node-1")
				}
				if tc.power != "" {
					pool.setPowerState(1, tc.power)
				}
				time.Sleep(tc.wait)
				if tc.restart {
					loop = newResumeLoop(t, client, pool, backup)
				}
				if tc.wait > 0 && tc.wait < 15*time.Minute {
					loop.runOnce(t)
					require.False(t, loop.backedOff(t))
					require.Empty(t, backup.history())
					require.Equal(t, []string{"start:1"}, pool.history())
					require.Contains(t, loop.snapshotPods(t, "node-1"), "pending")
					loop.requireSteadyResume(t, "node-1")
					op, pending := loop.pool.powerOperation("vm-1")
					require.True(t, pending)
					require.True(t, op.resumed.Equal(started))
					time.Sleep(time.Until(started.Add(15*time.Minute + time.Second)))
				}
				loop.runOnce(t)
				require.True(t, loop.backedOff(t))
				require.Len(t, loop.failures.Get()[loop.pool.Id()], 1)
				code := "start-deallocated-timeout"
				if tc.wait == 0 {
					code = "start-deallocated-failed"
				}
				require.Equal(t, code, loop.failures.Get()[loop.pool.Id()][0].ErrorInfo.ErrorCode)
				require.Equal(t, []string{"start:1", "deallocate:1"}, pool.history())
				require.Equal(t, []string{"grow:1"}, backup.history())
				require.Empty(t, loop.manager.resumingNodes())
				require.NotContains(t, loop.snapshotPods(t, "node-1"), "pending")
				loop.runOnce(t)
				reparked := loop.node(t, "node-1")
				require.Equal(t, retained.UID, reparked.UID)
				require.True(t, hasSuspendedMetadata(reparked))
				require.NotContains(t, reparked.Annotations, resumeMarkerAnnotation)
				require.Equal(t, vmPowerStateDeallocated, pool.states[1])
				require.Len(t, loop.failures.Get()[loop.pool.Id()], 1)
			})
		})
	}
}

func TestSuspendedLoopMissingOrReplacedNode(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "replaced"}[replace], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pool := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateDeallocated}}
				backup := &suspendedWorld{name: "backup"}
				client := newResumeCluster(t, pool)
				loop := newResumeLoop(t, client, pool, backup)
				loop.runOnce(t)
				require.Equal(t, []string{"start:1"}, pool.history())
				require.NoError(t, client.CoreV1().Pods("default").Delete(t.Context(), "pending", metav1.DeleteOptions{}))

				old := loop.node(t, "node-1")
				require.NoError(t, client.CoreV1().Nodes().Delete(t.Context(), old.Name, metav1.DeleteOptions{}))
				if replace {
					fresh := suspendedPoolNode(pool, 1, false)
					fresh.UID = "replacement"
					catest.SetNodeReadyState(fresh, false, time.Now())
					_, err := client.CoreV1().Nodes().Create(t.Context(), fresh, metav1.CreateOptions{})
					require.NoError(t, err)
				}
				backupPod := catest.BuildTestPod("backup-pending", 1000, 1024, catest.MarkUnschedulable(),
					catest.WithCreationTimestamp(time.Now().Add(-time.Minute)))
				backupPod.Spec.NodeSelector = map[string]string{"pool": "backup"}
				_, err := client.CoreV1().Pods(backupPod.Namespace).Create(t.Context(), backupPod, metav1.CreateOptions{})
				require.NoError(t, err)

				time.Sleep(time.Minute)
				loop.runOnce(t)
				require.Empty(t, loop.manager.resumingNodes(), "the resume of a missing or replaced Node is retired")
				require.Equal(t, []string{"grow:1"}, backup.history(), "another group still scales")
				if replace {
					current := loop.node(t, "node-1")
					require.Nil(t, nodeSuspended(current), "a replacement Node is never suspended")
					require.NotContains(t, current.Annotations, resumeMarkerAnnotation)
				}
			})
		})
	}
}

func TestSuspendedParkDuringResumeDoesNotRelistNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := &suspendedWorld{states: []string{vmPowerStateDeallocated, vmPowerStateRunning}}
		resuming, other := suspendedTestNode(0, true), suspendedTestNode(1, false)
		catest.SetNodeReadyState(resuming, false, time.Now())
		client := fake.NewClientset(resuming, other)
		_, group := newSuspendedProvider(t, world, client, 0, 3)
		require.NoError(t, group.IncreaseSize(t.Context(), 1))
		synctest.Wait()
		lists := 0
		client.PrependReactor("list", "nodes", func(clienttesting.Action) (bool, runtime.Object, error) {
			lists++
			if lists > 1 {
				return true, nil, errors.New("unexpected extra Node list")
			}
			return false, nil, nil
		})
		require.NoError(t, group.DeleteNodes(t.Context(), []*apiv1.Node{other}))
		require.Equal(t, 1, lists)
	})
}

func TestSuspendedResumeWaitsForParkHeadroom(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		active, parked := suspendedTestNode(0, false), suspendedTestNode(2, true)
		client := fake.NewClientset(active, parked)
		finish := make(chan struct{})
		world := &suspendedWorld{
			states:     []string{vmPowerStateRunning, vmPowerStateRunning, vmPowerStateDeallocated},
			parkResult: func(int) error { <-finish; return nil },
		}
		_, group := newSuspendedProvider(t, world, client, 0, 2)
		result := make(chan error, 1)
		completed := false
		defer func() {
			if !completed {
				close(finish)
				<-result
			}
		}()
		go func() { result <- group.DeleteNodes(t.Context(), []*apiv1.Node{active}) }()
		synctest.Wait()
		require.ErrorContains(t, group.IncreaseSize(t.Context(), 1), "parking is still in progress")
		require.Equal(t, []string{"deallocate:0"}, world.history())
		require.NotContains(t, getTestNode(t, client, parked.Name).Annotations, resumeMarkerAnnotation)

		close(finish)
		err := <-result
		completed = true
		require.NoError(t, err)
		require.NoError(t, group.IncreaseSize(t.Context(), 1))
		synctest.Wait()
		require.Equal(t, []string{"deallocate:0", "start:0"}, world.history())
	})
}

func TestSuspendedReparkDuringIncreaseAllowsGrowth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		node := suspendedTestNode(0, true)
		catest.SetNodeReadyState(node, false, time.Now())
		world := &suspendedWorld{
			states:      []string{vmPowerStateDeallocated, vmPowerStateRunning, vmPowerStateRunning},
			startResult: func(int) error { return &azcore.ResponseError{StatusCode: http.StatusOK} },
			parkError:   map[int]error{0: &azcore.ResponseError{StatusCode: http.StatusConflict}},
		}
		_, group := newSuspendedProvider(t, world, fake.NewClientset(node), 0, 3)
		require.NoError(t, group.IncreaseSize(t.Context(), 1))
		synctest.Wait()
		require.NoError(t, group.IncreaseSize(t.Context(), 1))
		require.Equal(t, []string{"start:0", "deallocate:0", "grow:4"}, world.history())
	})
}

func TestSuspendedResumePerVMDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := &suspendedWorld{
			states:     []string{vmPowerStateDeallocated, vmPowerStateDeallocated, vmPowerStateDeallocated},
			startDelay: 2 * time.Minute,
		}
		client := fake.NewClientset(suspendedTestNode(0, true), suspendedTestNode(1, true), suspendedTestNode(2, true))
		_, group := newSuspendedProvider(t, world, client, 0, 3)
		start := time.Now()
		require.NoError(t, group.IncreaseSize(t.Context(), 3))
		synctest.Wait()
		require.Equal(t, 6*time.Minute, time.Since(start))
		require.Equal(t, []string{"start:0", "start:1", "start:2"}, world.history())
	})
}

func TestSuspendedResumeRefreshUsesSharedNodeList(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := &suspendedWorld{states: []string{vmPowerStateDeallocated, vmPowerStateDeallocated}}
		nodes := []*apiv1.Node{suspendedTestNode(0, true), suspendedTestNode(1, true)}
		for _, node := range nodes {
			catest.SetNodeReadyState(node, false, time.Now())
		}
		client := fake.NewClientset(nodes[0], nodes[1])
		provider, group := newSuspendedProvider(t, world, client, 0, 3)
		require.NoError(t, group.IncreaseSize(t.Context(), 2))
		synctest.Wait()
		for range 2 {
			client.ClearActions()
			require.NoError(t, provider.Refresh(t.Context()))
			actions := client.Actions()
			require.Len(t, actions, 1)
			require.Equal(t, "list", actions[0].GetVerb())
		}
	})
}

func TestSuspendedScaleDownSkipsFailedResume(t *testing.T) {
	node := suspendedTestNode(0, true)
	world := &suspendedWorld{states: []string{vmPowerStateDeallocated}}
	provider, group := newSuspendedProvider(t, world, fake.NewClientset(node), 0, 1)
	// A resume that fails after the loop's snapshot made its Node schedulable stays excluded.
	group.recordPower("vm-0", suspendedPowerOperation{node: node, resumed: time.Now(), failed: true})

	require.Empty(t, provider.azureManager.resumingNodes())
	processor := &resumeScaleDownNodeProcessor{manager: provider.azureManager}
	require.Empty(t, processor.withoutResumes([]*apiv1.Node{node}))
}

func TestSuspendedConfigureAutoscalerDefaults(t *testing.T) {
	options := config.AutoscalingOptions{
		NodeGroupDefaults: config.NodeGroupAutoscalingOptions{MaxNodeProvisionTime: 20 * time.Minute},
	}
	processors := ca_processors.DefaultProcessors(options)
	client := fake.NewClientset()
	manager := &AzureManager{}

	manager.configureAutoscaler(&coreoptions.AutoscalerOptions{
		AutoscalingOptions: options, KubeClient: client, Processors: processors,
	})

	require.Equal(t, 20*time.Minute, manager.nodeGroupDefaults.MaxNodeProvisionTime)
	require.Same(t, client, manager.kubeClient)
}

func TestSuspendedResumeSnapshotKeepsDRAAndCSI(t *testing.T) {
	node := suspendedTestNode(0, true)
	custom := apiv1.Taint{Key: "example.com/keep", Effect: apiv1.TaintEffectNoSchedule}
	node.Spec.Taints = append(node.Spec.Taints, custom,
		apiv1.Taint{Key: "node.cloudprovider.kubernetes.io/shutdown", Effect: apiv1.TaintEffectNoSchedule},
		apiv1.Taint{Key: apiv1.TaintNodeUnreachable, Effect: apiv1.TaintEffectNoExecute})
	catest.SetNodeReadyState(node, false, time.Now())
	client := fake.NewClientset(node)
	_, group := newSuspendedProvider(t, &suspendedWorld{states: []string{vmPowerStateRunning}}, client, 0, 1)
	group.recordPower("vm-0", suspendedPowerOperation{node: node, resumed: time.Now()})

	handle, err := framework.NewHandle(t.Context(), informers.NewSharedInformerFactory(fake.NewClientset(), 0), nil, true, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handle.Framework.Close()) })
	snapshot := predicate.NewPredicateSnapshot(store.NewDeltaSnapshotStore(), handle, true, 1, true, 0)
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-slice"},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &node.Name, Driver: "gpu.example.com",
			Pool:    resourceapi.ResourcePool{Name: node.Name, ResourceSliceCount: 1},
			Devices: []resourceapi.Device{{Name: "gpu-0"}},
		},
	}
	csiNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: node.Name},
		Spec:       storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: "disk.csi.azure.com", NodeID: node.Name}}},
	}
	bound := catest.BuildTestPod("daemon", 100, 100, catest.WithNodeName(node.Name))
	require.NoError(t, snapshot.SetClusterState(t.Context(), []*apiv1.Node{node}, []*apiv1.Pod{bound},
		drasnapshot.NewSnapshot(nil, map[string][]*resourceapi.ResourceSlice{node.Name: {slice}}, nil, nil),
		csisnapshot.NewSnapshot(map[string]*storagev1.CSINode{node.Name: csiNode})))

	processor := &resumePodListProcessor{PodListProcessor: pods.NewDefaultPodListProcessor(), manager: group.manager}
	_, err = processor.Process(t.Context(), &ca_context.AutoscalingContext{ClusterSnapshot: snapshot}, nil)
	require.NoError(t, err)

	nodeInfo, err := snapshot.GetNodeInfo(node.Name)
	require.NoError(t, err)
	require.Equal(t, []apiv1.Taint{custom}, nodeInfo.Node().Spec.Taints)
	readiness, err := kube_util.GetNodeReadiness(nodeInfo.Node())
	require.NoError(t, err)
	require.True(t, readiness.Ready)
	require.Equal(t, []*resourceapi.ResourceSlice{slice}, nodeInfo.LocalResourceSlices)
	require.Equal(t, csiNode, nodeInfo.CSINode)
	require.Len(t, nodeInfo.Pods(), 1)
	require.Equal(t, bound.Name, nodeInfo.Pods()[0].Name)
	slices, found := snapshot.DraSnapshot().NodeResourceSlices(node.Name)
	require.True(t, found)
	require.Equal(t, []*resourceapi.ResourceSlice{slice}, slices)
	stored, err := snapshot.CsiSnapshot().Get(node.Name)
	require.NoError(t, err)
	require.Equal(t, csiNode, stored)

	current := getTestNode(t, client, node.Name)
	require.Equal(t, node, current, "only the snapshot changes")
}

func TestSuspendedLoopConcurrentResumes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := &suspendedWorld{
			states: []string{vmPowerStateRunning, vmPowerStateDeallocated, vmPowerStateDeallocated},
		}
		pool.startResult = func(i int) error {
			if i == 2 {
				return &azcore.ResponseError{StatusCode: http.StatusOK}
			}
			pool.setPowerState(i, vmPowerStateRunning)
			return nil
		}
		backup := &suspendedWorld{name: "backup"}
		client := newResumeCluster(t, pool)
		require.NoError(t, client.CoreV1().Pods("default").Delete(t.Context(), "pending", metav1.DeleteOptions{}))
		for _, name := range []string{"first", "second"} {
			// Each pod needs its own Node.
			spread := map[string]string{"app": "spread"}
			pod := catest.BuildTestPod(name, 1000, 1024, catest.MarkUnschedulable(),
				catest.WithCreationTimestamp(time.Now().Add(-time.Minute)),
				catest.WithLabels(spread), catest.WithPodHostnameAntiAffinity(spread))
			_, err := client.CoreV1().Pods(pod.Namespace).Create(t.Context(), pod, metav1.CreateOptions{})
			require.NoError(t, err)
		}
		loop := newResumeLoop(t, client, pool, backup)

		loop.runOnce(t)
		require.ElementsMatch(t, []string{"start:1", "start:2"}, pool.history())

		loop.runOnce(t)
		require.True(t, loop.backedOff(t))
		require.Len(t, loop.failures.Get()[loop.pool.Id()], 1)
		require.Equal(t, map[string]types.UID{"node-1": "old-node-1"}, loop.manager.resumingNodes(),
			"the other resume continues")
		require.Len(t, loop.snapshotPods(t, "node-1"), 1, "one pending pod packs onto the healthy resume")
		require.Equal(t, []string{"grow:1"}, backup.history(), "the other pod falls back to the backup group")
		loop.requireSteadyResume(t, "node-1")

		loop.setReady(t, "node-1")
		loop.runOnce(t)
		loop.runOnce(t)
		failed := loop.node(t, "node-2")
		require.True(t, hasSuspendedMetadata(failed))
		require.NotContains(t, failed.Annotations, resumeMarkerAnnotation)
		require.Empty(t, loop.manager.resumingNodes())
		require.False(t, hasSuspendedMetadata(loop.node(t, "node-1")))
		require.ElementsMatch(t, []string{"start:1", "start:2", "deallocate:2"}, pool.history())
		require.Equal(t, []string{"grow:1"}, backup.history())
		require.Len(t, loop.failures.Get()[loop.pool.Id()], 1)
	})
}

func TestSuspendedLoopEnforcesMinimumByResuming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := &suspendedWorld{states: []string{vmPowerStateRunning, vmPowerStateDeallocated, vmPowerStateDeallocated}}
		backup := &suspendedWorld{name: "backup"}
		client := newResumeCluster(t, pool)
		require.NoError(t, client.CoreV1().Pods("default").Delete(t.Context(), "pending", metav1.DeleteOptions{}))
		loop := newResumeLoop(t, client, pool, backup, func(spec *dynamic.NodeGroupSpec, options *config.AutoscalingOptions) {
			spec.MinSize = 2
			options.EnforceNodeGroupMinSize = true
		})

		loop.runOnce(t)
		require.Equal(t, []string{"start:1"}, pool.history(), "minimum enforcement resumes a parked VM")
		for range 5 {
			time.Sleep(time.Minute)
			loop.runOnce(t)
			require.Equal(t, []string{"start:1"}, pool.history(), "an in-flight resume counts toward the minimum")
			loop.requireSteadyResume(t, "node-1")
		}
		target, err := loop.pool.TargetSize(t.Context())
		require.NoError(t, err)
		require.Equal(t, target, loop.pool.MinSize(t.Context()))

		loop.setReady(t, "node-1")
		loop.runOnce(t)
		loop.runOnce(t)
		require.False(t, hasSuspendedMetadata(loop.node(t, "node-1")))
		require.Equal(t, []string{"start:1"}, pool.history())
		require.Empty(t, backup.history())
		require.False(t, loop.backedOff(t))
	})
}
