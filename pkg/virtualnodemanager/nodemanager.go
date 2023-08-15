package virtualnodemanager

import (
	"context"
	"github.com/clusterrouter-io/clusterrouter/cmd/virtualnode-manager/app/config"
	virtualnodev1alpha1 "github.com/clusterrouter-io/clusterrouter/pkg/api/clusterrouter.io/v1alpha1"
	crdclientset "github.com/clusterrouter-io/clusterrouter/pkg/generated/clientset/versioned"
	"github.com/clusterrouter-io/clusterrouter/pkg/generated/informers/externalversions"
	vnlister "github.com/clusterrouter-io/clusterrouter/pkg/generated/listers/clusterrouter.io/v1alpha1"
	"github.com/clusterrouter-io/clusterrouter/pkg/plugins/virtualk8s"
	"github.com/clusterrouter-io/clusterrouter/pkg/virtualnodemanager/virtualnode"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"math"
	"math/rand"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sync"
	"time"
)

const VirtualNodeControllerFinalizer = "clusterrouter.io/virtual-node-controller"
const defaultRetryNum = 5

type Manager struct {
	runLock sync.Mutex
	stopCh  <-chan struct{}

	vnclient        crdclientset.Interface
	informerFactory externalversions.SharedInformerFactory

	queue      workqueue.RateLimitingInterface
	vnLister   vnlister.VirtualNodeLister
	vnInformer cache.SharedIndexInformer

	vnlock       sync.RWMutex
	virtualNodes map[string]*virtualnode.VirtualNode
	vnWaitGroup  wait.Group

	opts *config.Opts
}

func NewManager(c *config.Config) *Manager {
	factory := externalversions.NewSharedInformerFactory(c.CRDClient, 0)
	vnInformer := factory.Clusterrouter().V1alpha1().VirtualNodes()

	manager := &Manager{
		vnclient:        c.CRDClient,
		informerFactory: factory,
		vnLister:        vnInformer.Lister(),
		vnInformer:      vnInformer.Informer(),

		queue: workqueue.NewRateLimitingQueue(
			NewItemExponentialFailureAndJitterSlowRateLimter(2*time.Second, 15*time.Second, 1*time.Minute, 1.0, defaultRetryNum),
		),
		virtualNodes: make(map[string]*virtualnode.VirtualNode),
		opts:         c.Opts,
	}

	vnInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    manager.addCluster,
			UpdateFunc: manager.updateCluster,
			DeleteFunc: manager.deleteCluster,
		},
	)

	return manager
}

func (manager *Manager) addCluster(obj interface{}) {
	manager.enqueue(obj)
}

func (manager *Manager) updateCluster(older, newer interface{}) {
	oldObj := older.(*virtualnodev1alpha1.VirtualNode)
	newObj := newer.(*virtualnodev1alpha1.VirtualNode)
	if newObj.DeletionTimestamp.IsZero() && equality.Semantic.DeepEqual(oldObj.Spec, newObj.Spec) {
		return
	}

	manager.enqueue(newer)
}

func (manager *Manager) deleteCluster(obj interface{}) {
	manager.enqueue(obj)
}

func (manager *Manager) enqueue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}

	manager.queue.Add(key)
}

func (manager *Manager) processNextCluster() (continued bool) {
	key, shutdown := manager.queue.Get()
	if shutdown {
		return false
	}
	defer manager.queue.Done(key)
	continued = true

	_, name, err := cache.SplitMetaNamespaceKey(key.(string))
	if err != nil {
		klog.Error(err)
		return
	}

	klog.InfoS("reconcile cluster", "virtualNode", name)
	vNode, err := manager.vnLister.Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.InfoS("cluster has been deleted", "virtual node", name)
			return
		}

		klog.ErrorS(err, "Failed to get cluster from cache", "cluvirtual node", name)
		return
	}

	vNode = vNode.DeepCopy()
	if result := manager.reconcileVNode(vNode); result.Requeue() {
		if num := manager.queue.NumRequeues(key); num < result.MaxRetryCount() {
			klog.V(3).InfoS("requeue cluster", "cluster", name, "num requeues", num+1)
			manager.queue.AddRateLimited(key)
			return
		}
		klog.V(2).Infof("Dropping cluster %q out of the queue: %v", key, err)
	}
	manager.queue.Forget(key)
	return
}

func (manager *Manager) worker() {
	for manager.processNextCluster() {
		select {
		case <-manager.stopCh:
			return
		default:
		}
	}
}

func (manager *Manager) Run(workers int, stopCh <-chan struct{}) {
	manager.runLock.Lock()
	defer manager.runLock.Unlock()
	if manager.stopCh != nil {
		klog.Fatal("virtualnode manager is already running...")
	}
	klog.Info("Start Informer Factory")

	// informerFactory should not be controlled by stopCh
	stopInformer := make(chan struct{})
	manager.informerFactory.Start(stopInformer)
	if !cache.WaitForCacheSync(stopCh, manager.vnInformer.HasSynced) {
		klog.Fatal("virtualnode manager: wait for informer factory failed")
	}

	manager.stopCh = stopCh

	klog.InfoS("Start Manager Cluster Worker", "workers", workers)
	var waitGroup sync.WaitGroup
	for i := 0; i < workers; i++ {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()
			wait.Until(manager.worker, time.Second, manager.stopCh)
		}()
	}

	<-manager.stopCh
	klog.Info("receive stop signal, stop...")

	manager.queue.ShutDown()
	waitGroup.Wait()

	klog.Info("wait for cluster synchros stop...")
	manager.vnWaitGroup.Wait()
	klog.Info("cluster synchro manager stopped.")
}

// if err returned is not nil, cluster will be requeued
func (manager *Manager) reconcileVNode(vNode *virtualnodev1alpha1.VirtualNode) Result {
	if !vNode.DeletionTimestamp.IsZero() {
		klog.InfoS("remove vNode", "vNode", vNode.Name)
		if err := manager.removeVNode(vNode.Name); err != nil {
			klog.ErrorS(err, "Failed to remove virtula node", vNode.Name)
			return RequeueResult(defaultRetryNum)
		}

		if !controllerutil.ContainsFinalizer(vNode, VirtualNodeControllerFinalizer) {
			return NoRequeueResult
		}

		// remove finalizer
		controllerutil.RemoveFinalizer(vNode, VirtualNodeControllerFinalizer)
		if _, err := manager.vnclient.ClusterrouterV1alpha1().VirtualNodes().Update(context.TODO(), vNode, metav1.UpdateOptions{}); err != nil {
			klog.ErrorS(err, "Failed to remove finalizer", "virtual node", vNode.Name)
			return RequeueResult(defaultRetryNum)
		}
		return NoRequeueResult
	}

	// ensure finalizer
	if !controllerutil.ContainsFinalizer(vNode, VirtualNodeControllerFinalizer) {
		controllerutil.AddFinalizer(vNode, VirtualNodeControllerFinalizer)

		if _, err := manager.vnclient.ClusterrouterV1alpha1().VirtualNodes().Update(context.TODO(), vNode, metav1.UpdateOptions{}); err != nil {
			klog.ErrorS(err, "Failed to add finalizer", "virtual node", vNode.Name)
			return RequeueResult(defaultRetryNum)
		}
	}

	manager.vnlock.RLock()
	virtualNode := manager.virtualNodes[vNode.Name]
	manager.vnlock.RUnlock()

	if virtualNode == nil {
		cc := virtualk8s.ClientConfig{
			KubeClientQPS:    500,
			KubeClientBurst:  1000,
			ClientKubeConfig: vNode.Spec.Kubeconfig,
		}

		var opts config.Opts
		opts = *manager.opts
		opts.Provider = vNode.Spec.Type
		opts.NodeName = vNode.Spec.NodeName
		opts.DisableTaint = vNode.Spec.DisableTaint

		ctx := context.TODO()

		var err error
		virtualNode, err = virtualnode.NewVirtualNode(ctx, &cc, &opts)
		if err != nil {
			klog.ErrorS(err, "Failed to new virtualnode", "vitrul node", vNode.Name)
			/*		manager.UpdateClusterAPIServerAndValidatedCondition(cluster.Name, cluster.Spec.APIServer, nil, synchro, clusterv1alpha2.InvalidConfigReason,
					"invalid cluster config: "+err.Error(), metav1.ConditionFalse)*/
			return NoRequeueResult
		}
		go virtualNode.Run(ctx, &opts)

		manager.vnlock.RLock()
		manager.virtualNodes[vNode.Name] = virtualNode
		manager.vnlock.RUnlock()
	}

	return NoRequeueResult
}

func (manager *Manager) removeVNode(name string) error {
	manager.vnlock.Lock()
	vNode := manager.virtualNodes[name]
	delete(manager.virtualNodes, name)
	manager.vnlock.Unlock()

	if vNode != nil {
		// not update removed cluster status,
		// and ensure that no more data is being synchronized to the resource storage
		vNode.Shutdown()
	}

	// clean cluster from storage
	return nil
}

func NewItemExponentialFailureAndJitterSlowRateLimter(fastBaseDelay, fastMaxDelay, slowBaseDeploy time.Duration, slowMaxFactor float64, maxFastAttempts int) workqueue.RateLimiter {
	if slowMaxFactor <= 0.0 {
		slowMaxFactor = 1.0
	}
	return &ItemExponentialFailureAndJitterSlowRateLimter{
		failures:        map[interface{}]int{},
		maxFastAttempts: maxFastAttempts,
		fastBaseDelay:   fastBaseDelay,
		fastMaxDelay:    fastMaxDelay,
		slowBaseDelay:   slowBaseDeploy,
		slowMaxFactor:   slowMaxFactor,
	}
}

type ItemExponentialFailureAndJitterSlowRateLimter struct {
	failuresLock sync.Mutex
	failures     map[interface{}]int

	maxFastAttempts int

	fastBaseDelay time.Duration
	fastMaxDelay  time.Duration

	slowBaseDelay time.Duration
	slowMaxFactor float64
}

func (r *ItemExponentialFailureAndJitterSlowRateLimter) When(item interface{}) time.Duration {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	fastExp, num := r.failures[item], r.failures[item]+1
	r.failures[item] = num
	if num > r.maxFastAttempts {
		return r.slowBaseDelay + time.Duration(rand.Float64()*r.slowMaxFactor*float64(r.slowBaseDelay))
	}

	// The backoff is capped such that 'calculated' value never overflows.
	backoff := float64(r.fastBaseDelay.Nanoseconds()) * math.Pow(2, float64(fastExp))
	if backoff > math.MaxInt64 {
		return r.fastMaxDelay
	}

	calculated := time.Duration(backoff)
	if calculated > r.fastMaxDelay {
		return r.fastMaxDelay
	}
	return calculated
}

func (r *ItemExponentialFailureAndJitterSlowRateLimter) NumRequeues(item interface{}) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	return r.failures[item]
}

func (r *ItemExponentialFailureAndJitterSlowRateLimter) Forget(item interface{}) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	delete(r.failures, item)
}
