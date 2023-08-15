package controllers

import (
	"context"
	"fmt"
	"github.com/clusterrouter-io/clusterrouter/pkg/plugins"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/errdefs"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/podutils"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/queue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1informers "k8s.io/client-go/informers/core/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"github.com/clusterrouter-io/clusterrouter/pkg/utils/log"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/manager"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/trace"
	"github.com/google/go-cmp/cmp"
	pkgerrors "github.com/pkg/errors"
)

const (
	podStatusReasonProviderFailed = "ProviderFailed"
	podEventCreateFailed          = "ProviderCreateFailed"
	podEventCreateSuccess         = "ProviderCreateSuccess"
	podEventDeleteFailed          = "ProviderDeleteFailed"
	podEventDeleteSuccess         = "ProviderDeleteSuccess"
	podEventUpdateFailed          = "ProviderUpdateFailed"
	podEventUpdateSuccess         = "ProviderUpdateSuccess"

	// 151 milliseconds is just chosen as a small prime number to retry between
	// attempts to get a notification from the provider to VK
	notificationRetryPeriod = 151 * time.Millisecond
)

// PodEventFilterFunc is used to filter pod events received from Kubernetes.
//
// Filters that return true means the event handler will be run
// Filters that return false means the filter will *not* be run.
type PodEventFilterFunc func(context.Context, *corev1.Pod) bool

// PodController is the controller implementation for Pod resources.
type PodController struct {
	provider plugins.PodLifecycleHandler

	// podsInformer is an informer for Pod resources.
	podsInformer corev1informers.PodInformer
	// podsLister is able to list/get Pod resources from a shared informer's store.
	podsLister corev1listers.PodLister

	// recorder is an event recorder for recording Event resources to the Kubernetes API.
	recorder record.EventRecorder

	client corev1client.PodsGetter

	resourceManager *manager.ResourceManager

	syncPodsFromKubernetes *queue.Queue

	// deletePodsFromKubernetes is a queue on which pods are reconciled, and we check if pods are in API server after
	// the grace period
	deletePodsFromKubernetes *queue.Queue

	syncPodStatusFromProvider *queue.Queue

	// From the time of creation, to termination the knownPods map will contain the pods key
	// (derived from Kubernetes' cache library) -> a *knownPod struct.
	knownPods sync.Map

	podEventFilterFunc PodEventFilterFunc

	// ready is a channel which will be closed once the pod controller is fully up and running.
	// this channel will never be closed if there is an error on startup.
	ready chan struct{}
	// done is closed when Run returns
	// Once done is closed `err` may be set to a non-nil value
	done chan struct{}

	mu sync.Mutex
	// err is set if there is an error while while running the pod controller.
	// Typically this would be errors that occur during startup.
	// Once err is set, `Run` should return.
	//
	// This is used since `pc.Run()` is typically called in a goroutine and managing
	// this can be non-trivial for callers.
	err error

	pf informers.SharedInformerFactory
}

type knownPod struct {
	// You cannot read (or modify) the fields in this struct without taking the lock. The individual fields
	// should be immutable to avoid having to hold the lock the entire time you're working with them
	sync.Mutex
	lastPodStatusReceivedFromProvider *corev1.Pod
	lastPodUsed                       *corev1.Pod
	lastPodStatusUpdateSkipped        bool
}

// PodControllerConfig is used to configure a new PodController.
type PodControllerConfig struct {
	// PodClient is used to perform actions on the k8s API, such as updating pod status
	// This field is required
	PodClient corev1client.PodsGetter

	// PodInformer is used as a local cache for pods
	// This should be configured to only look at pods scheduled to the node which the controller will be managing
	// If the informer does not filter based on node, then you must provide a `PodEventFilterFunc` parameter so event handlers
	//   can filter pods not assigned to this node.
	PodInformer corev1informers.PodInformer

	EventRecorder record.EventRecorder

	Provider plugins.PodLifecycleHandler

	// Informers used for filling details for things like downward API in pod spec.
	//
	// We are using informers here instead of listers because we'll need the
	// informer for certain features (like notifications for updated ConfigMaps)
	ConfigMapInformer corev1informers.ConfigMapInformer
	SecretInformer    corev1informers.SecretInformer
	ServiceInformer   corev1informers.ServiceInformer

	// SyncPodsFromKubernetesRateLimiter defines the rate limit for the SyncPodsFromKubernetes queue
	SyncPodsFromKubernetesRateLimiter workqueue.RateLimiter
	// DeletePodsFromKubernetesRateLimiter defines the rate limit for the DeletePodsFromKubernetesRateLimiter queue
	DeletePodsFromKubernetesRateLimiter workqueue.RateLimiter
	// SyncPodStatusFromProviderRateLimiter defines the rate limit for the SyncPodStatusFromProviderRateLimiter queue
	SyncPodStatusFromProviderRateLimiter workqueue.RateLimiter

	// Add custom filtering for pod informer event handlers
	// Use this for cases where the pod informer handles more than pods assigned to this node
	//
	// For example, if the pod informer is not filtering based on pod.Spec.NodeName, you should
	// set that filter here so the pod controller does not handle events for pods assigned to other nodes.
	PodEventFilterFunc PodEventFilterFunc
}

// NewPodController creates a new pod controller with the provided config.
func NewPodController(cfg PodControllerConfig) (*PodController, error) {
	if cfg.PodClient == nil {
		return nil, errdefs.InvalidInput("missing core client")
	}
	if cfg.EventRecorder == nil {
		return nil, errdefs.InvalidInput("missing event recorder")
	}
	if cfg.PodInformer == nil {
		return nil, errdefs.InvalidInput("missing pod informer")
	}
	if cfg.ConfigMapInformer == nil {
		return nil, errdefs.InvalidInput("missing config map informer")
	}
	if cfg.SecretInformer == nil {
		return nil, errdefs.InvalidInput("missing secret informer")
	}
	if cfg.ServiceInformer == nil {
		return nil, errdefs.InvalidInput("missing service informer")
	}
	if cfg.Provider == nil {
		return nil, errdefs.InvalidInput("missing provider")
	}
	if cfg.SyncPodsFromKubernetesRateLimiter == nil {
		cfg.SyncPodsFromKubernetesRateLimiter = workqueue.DefaultControllerRateLimiter()
	}
	if cfg.DeletePodsFromKubernetesRateLimiter == nil {
		cfg.DeletePodsFromKubernetesRateLimiter = workqueue.DefaultControllerRateLimiter()
	}
	if cfg.SyncPodStatusFromProviderRateLimiter == nil {
		cfg.SyncPodStatusFromProviderRateLimiter = workqueue.DefaultControllerRateLimiter()
	}
	rm, err := manager.NewResourceManager(cfg.PodInformer.Lister(), cfg.SecretInformer.Lister(), cfg.ConfigMapInformer.Lister(), cfg.ServiceInformer.Lister())
	if err != nil {
		return nil, pkgerrors.Wrap(err, "could not create resource manager")
	}

	pc := &PodController{
		client:             cfg.PodClient,
		podsInformer:       cfg.PodInformer,
		podsLister:         cfg.PodInformer.Lister(),
		provider:           cfg.Provider,
		resourceManager:    rm,
		ready:              make(chan struct{}),
		done:               make(chan struct{}),
		recorder:           cfg.EventRecorder,
		podEventFilterFunc: cfg.PodEventFilterFunc,
	}

	pc.syncPodsFromKubernetes = queue.New(cfg.SyncPodsFromKubernetesRateLimiter, "syncPodsFromKubernetes", pc.syncPodFromKubernetesHandler)
	pc.deletePodsFromKubernetes = queue.New(cfg.DeletePodsFromKubernetesRateLimiter, "deletePodsFromKubernetes", pc.deletePodsFromKubernetesHandler)
	pc.syncPodStatusFromProvider = queue.New(cfg.SyncPodStatusFromProviderRateLimiter, "syncPodStatusFromProvider", pc.syncPodStatusFromProviderHandler)

	return pc, nil
}

type asyncProvider interface {
	plugins.PodLifecycleHandler
	plugins.PodNotifier
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers.  It will block until the
// context is cancelled, at which point it will shutdown the work queue and
// wait for workers to finish processing their current work items prior to
// returning.
//
// Once this returns, you should not re-use the controller.
func (pc *PodController) Run(ctx context.Context, podSyncWorkers int) (retErr error) {
	// Shutdowns are idempotent, so we can call it multiple times. This is in case we have to bail out early for some reason.
	// This is to make extra sure that any workers we started are terminated on exit
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer func() {
		pc.mu.Lock()
		pc.err = retErr
		close(pc.done)
		pc.mu.Unlock()
	}()

	var provider asyncProvider
	runProvider := func(context.Context) {}

	if p, ok := pc.provider.(asyncProvider); ok {
		provider = p
	} else {
		wrapped := &syncProviderWrapper{PodLifecycleHandler: pc.provider, l: pc.podsLister}
		runProvider = wrapped.run
		provider = wrapped
		log.G(ctx).Debug("Wrapped non-async provider with async")
	}
	pc.provider = provider

	provider.NotifyPods(ctx, func(pod *corev1.Pod) {
		pc.enqueuePodStatusUpdate(ctx, pod.DeepCopy())
	})
	go runProvider(ctx)

	// Wait for the caches to be synced *before* starting to do work.
	if ok := cache.WaitForCacheSync(ctx.Done(), pc.podsInformer.Informer().HasSynced); !ok {
		return pkgerrors.New("failed to wait for caches to sync")
	}
	log.G(ctx).Info("Pod cache in-sync")

	// Set up event handlers for when Pod resources change. Since the pod cache is in-sync, the informer will generate
	// synthetic add events at this point. It again avoids the race condition of adding handlers while the cache is
	// syncing.

	var eventHandler cache.ResourceEventHandler = cache.ResourceEventHandlerFuncs{
		AddFunc: func(pod interface{}) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			ctx, span := trace.StartSpan(ctx, "AddFunc")
			defer span.End()

			if key, err := cache.MetaNamespaceKeyFunc(pod); err != nil {
				log.G(ctx).Error(err)
			} else {
				ctx = span.WithField(ctx, "key", key)
				pc.knownPods.Store(key, &knownPod{})
				pc.syncPodsFromKubernetes.Enqueue(ctx, key)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			ctx, span := trace.StartSpan(ctx, "UpdateFunc")
			defer span.End()

			// Create a copy of the old and new pod objects so we don't mutate the cache.
			oldPod := oldObj.(*corev1.Pod)
			newPod := newObj.(*corev1.Pod)

			// At this point we know that something in .metadata or .spec has changed, so we must proceed to sync the pod.
			if key, err := cache.MetaNamespaceKeyFunc(newPod); err != nil {
				log.G(ctx).Error(err)
			} else {
				ctx = span.WithField(ctx, "key", key)
				obj, ok := pc.knownPods.Load(key)
				if !ok {
					// Pods are only ever *added* to knownPods in the above AddFunc, and removed
					// in the below *DeleteFunc*
					panic("Pod not found in known pods. This should never happen.")
				}

				kPod := obj.(*knownPod)
				kPod.Lock()
				if kPod.lastPodStatusUpdateSkipped &&
					(!cmp.Equal(newPod.Status, kPod.lastPodStatusReceivedFromProvider.Status) ||
						!cmp.Equal(newPod.Annotations, kPod.lastPodStatusReceivedFromProvider.Annotations) ||
						!cmp.Equal(newPod.Labels, kPod.lastPodStatusReceivedFromProvider.Labels) ||
						!cmp.Equal(newPod.Finalizers, kPod.lastPodStatusReceivedFromProvider.Finalizers)) {
					// The last pod from the provider -> kube api server was skipped, but we see they no longer match.
					// This means that the pod in API server was changed by someone else [this can be okay], but we skipped
					// a status update on our side because we compared the status received from the provider to the status
					// received from the k8s api server based on outdated information.
					pc.syncPodStatusFromProvider.Enqueue(ctx, key)
					// Reset this to avoid re-adding it continuously
					kPod.lastPodStatusUpdateSkipped = false
				}
				kPod.Unlock()

				if podShouldEnqueue(oldPod, newPod) {
					pc.syncPodsFromKubernetes.Enqueue(ctx, key)
				}
			}
		},
		DeleteFunc: func(pod interface{}) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			ctx, span := trace.StartSpan(ctx, "DeleteFunc")
			defer span.End()

			if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(pod); err != nil {
				log.G(ctx).Error(err)
			} else {
				k8sPod, ok := pod.(*corev1.Pod)
				if !ok {
					return
				}
				ctx = span.WithField(ctx, "key", key)
				pc.knownPods.Delete(key)
				pc.syncPodsFromKubernetes.Enqueue(ctx, key)
				// If this pod was in the deletion queue, forget about it
				key = fmt.Sprintf("%v/%v", key, k8sPod.UID)
				pc.deletePodsFromKubernetes.Forget(ctx, key)
			}
		},
	}

	if pc.podEventFilterFunc != nil {
		eventHandler = cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				p, ok := obj.(*corev1.Pod)
				if !ok {
					return false
				}
				return pc.podEventFilterFunc(ctx, p)
			},
			Handler: eventHandler,
		}
	}

	pc.podsInformer.Informer().AddEventHandler(eventHandler)

	// Perform a reconciliation step that deletes any dangling pods from the provider.
	// This happens only when the cluster-router is starting, and operates on a "best-effort" basis.
	// If by any reason the provider fails to delete a dangling pod, it will stay in the provider and deletion won't be retried.
	pc.deleteDanglingPods(ctx, podSyncWorkers)

	log.G(ctx).Info("starting workers")
	group := &wait.Group{}
	group.StartWithContext(ctx, func(ctx context.Context) {
		pc.syncPodsFromKubernetes.Run(ctx, podSyncWorkers)
	})
	group.StartWithContext(ctx, func(ctx context.Context) {
		pc.deletePodsFromKubernetes.Run(ctx, podSyncWorkers)
	})
	group.StartWithContext(ctx, func(ctx context.Context) {
		pc.syncPodStatusFromProvider.Run(ctx, podSyncWorkers)
	})
	defer group.Wait()
	log.G(ctx).Info("podcontroller started workers")
	close(pc.ready)

	//pc.pf.Start(ctx.Done())

	<-ctx.Done()
	log.G(ctx).Info("shutting down podcontroller workers")

	return nil
}

// Ready returns a channel which gets closed once the PodController is ready to handle scheduled pods.
// This channel will never close if there is an error on startup.
// The status of this channel after shutdown is indeterminate.
func (pc *PodController) Ready() <-chan struct{} {
	return pc.ready
}

// Done returns a channel receiver which is closed when the pod controller has exited.
// Once the pod controller has exited you can call `pc.Err()` to see if any error occurred.
func (pc *PodController) Done() <-chan struct{} {
	return pc.done
}

// Err returns any error that has occurred and caused the pod controller to exit.
func (pc *PodController) Err() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.err
}

// syncPodFromKubernetesHandler compares the actual state with the desired, and attempts to converge the two.
func (pc *PodController) syncPodFromKubernetesHandler(ctx context.Context, key string) error {
	ctx, span := trace.StartSpan(ctx, "syncPodFromKubernetesHandler")
	defer span.End()

	// Add the current key as an attribute to the current span.
	ctx = span.WithField(ctx, "key", key)
	log.G(ctx).WithField("key", key).Debug("sync handled")

	// Convert the namespace/name string into a distinct namespace and name.
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// Log the error as a warning, but do not requeue the key as it is invalid.
		log.G(ctx).Warn(pkgerrors.Wrapf(err, "invalid resource key: %q", key))
		return nil
	}

	// Get the Pod resource with this namespace/name.
	pod, err := pc.podsLister.Pods(namespace).Get(name)
	if err != nil {
		if !errors.IsNotFound(err) {
			// We've failed to fetch the pod from the lister, but the error is not a 404.
			// Hence, we add the key back to the work queue so we can retry processing it later.
			err := pkgerrors.Wrapf(err, "failed to fetch pod with key %q from lister", key)
			span.SetStatus(err)
			return err
		}

		pod, err = pc.provider.GetPod(ctx, namespace, name)
		if err != nil && !errdefs.IsNotFound(err) {
			err = pkgerrors.Wrapf(err, "failed to fetch pod with key %q from provider", key)
			span.SetStatus(err)
			return err
		}
		if errdefs.IsNotFound(err) || pod == nil {
			return nil
		}

		err = pc.provider.DeletePod(ctx, pod)
		if errdefs.IsNotFound(err) {
			return nil
		}
		if err != nil {
			err = pkgerrors.Wrapf(err, "failed to delete pod %q in the provider", loggablePodNameFromCoordinates(namespace, name))
			span.SetStatus(err)
		}
		return err

	}

	// At this point we know the Pod resource has either been created or updated (which includes being marked for deletion).
	return pc.syncPodInProvider(ctx, pod, key)
}

// syncPodInProvider tries and reconciles the state of a pod by comparing its Kubernetes representation and the provider's representation.
func (pc *PodController) syncPodInProvider(ctx context.Context, pod *corev1.Pod, key string) (retErr error) {
	ctx, span := trace.StartSpan(ctx, "syncPodInProvider")
	defer span.End()

	// Add the pod's attributes to the current span.
	ctx = addPodAttributes(ctx, span, pod)

	if pod.DeletionTimestamp != nil && !running(&pod.Status) {
		log.G(ctx).Debug("Force deleting pod from API Server as it is no longer running")
		pc.deletePodsFromKubernetes.EnqueueWithoutRateLimit(ctx, key)
		key = fmt.Sprintf("%v/%v", key, pod.UID)
		pc.deletePodsFromKubernetes.EnqueueWithoutRateLimit(ctx, key)
		return nil
	}
	obj, ok := pc.knownPods.Load(key)
	if !ok {
		// That means the pod was deleted while we were working
		return nil
	}
	kPod := obj.(*knownPod)
	kPod.Lock()
	if kPod.lastPodUsed != nil && podsEffectivelyEqual(kPod.lastPodUsed, pod) {
		kPod.Unlock()
		return nil
	}
	kPod.Unlock()

	defer func() {
		if retErr == nil {
			kPod.Lock()
			defer kPod.Unlock()
			kPod.lastPodUsed = pod
		}
	}()

	// Check whether the pod has been marked for deletion.
	// If it does, guarantee it is deleted in the provider and Kubernetes.
	if pod.DeletionTimestamp != nil {
		log.G(ctx).Debug("Deleting pod in provider")
		if err := pc.deletePod(ctx, pod); errdefs.IsNotFound(err) {
			log.G(ctx).Debug("Pod not found in provider")
		} else if err != nil {
			err := pkgerrors.Wrapf(err, "failed to delete pod %q in the provider", loggablePodName(pod))
			span.SetStatus(err)
			return err
		}

		key = fmt.Sprintf("%v/%v", key, pod.UID)
		pc.deletePodsFromKubernetes.EnqueueWithoutRateLimitWithDelay(ctx, key, time.Second*time.Duration(*pod.DeletionGracePeriodSeconds))
		return nil
	}

	// Ignore the pod if it is in the "Failed" or "Succeeded" state.
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		log.G(ctx).Warnf("skipping sync of pod %q in %q phase", loggablePodName(pod), pod.Status.Phase)
		return nil
	}

	// Create or update the pod in the provider.
	if err := pc.createOrUpdatePod(ctx, pod); err != nil {
		err := pkgerrors.Wrapf(err, "failed to sync pod %q in the provider", loggablePodName(pod))
		span.SetStatus(err)
		return err
	}
	return nil
}

// deleteDanglingPods checks whether the provider knows about any pods which Kubernetes doesn't know about, and deletes them.
func (pc *PodController) deleteDanglingPods(ctx context.Context, threadiness int) {
	ctx, span := trace.StartSpan(ctx, "deleteDanglingPods")
	defer span.End()

	// Grab the list of pods known to the provider.
	pps, err := pc.provider.GetPods(ctx)
	if err != nil {
		err := pkgerrors.Wrap(err, "failed to fetch the list of pods from the provider")
		span.SetStatus(err)
		log.G(ctx).Error(err)
		return
	}

	// Create a slice to hold the pods we will be deleting from the provider.
	ptd := make([]*corev1.Pod, 0)

	// Iterate over the pods known to the provider, marking for deletion those that don't exist in Kubernetes.
	// Take on this opportunity to populate the list of key that correspond to pods known to the provider.
	for _, pp := range pps {
		if _, err := pc.podsLister.Pods(pp.Namespace).Get(pp.Name); err != nil {
			if errors.IsNotFound(err) {
				// The current pod does not exist in Kubernetes, so we mark it for deletion.
				ptd = append(ptd, pp)
				continue
			}
			// For some reason we couldn't fetch the pod from the lister, so we propagate the error.
			err := pkgerrors.Wrap(err, "failed to fetch pod from the lister")
			span.SetStatus(err)
			log.G(ctx).Error(err)
			return
		}
	}

	// We delete each pod in its own goroutine, allowing a maximum of "threadiness" concurrent deletions.
	semaphore := make(chan struct{}, threadiness)
	var wg sync.WaitGroup
	wg.Add(len(ptd))

	// Iterate over the slice of pods to be deleted and delete them in the provider.
	for _, pod := range ptd {
		go func(ctx context.Context, pod *corev1.Pod) {
			defer wg.Done()

			ctx, span := trace.StartSpan(ctx, "deleteDanglingPod")
			defer span.End()

			semaphore <- struct{}{}
			defer func() {
				<-semaphore
			}()

			// Add the pod's attributes to the current span.
			ctx = addPodAttributes(ctx, span, pod)
			// Actually delete the pod.
			if err := pc.provider.DeletePod(ctx, pod.DeepCopy()); err != nil && !errdefs.IsNotFound(err) {
				span.SetStatus(err)
				log.G(ctx).Errorf("failed to delete pod %q in provider", loggablePodName(pod))
			} else {
				log.G(ctx).Infof("deleted leaked pod %q in provider", loggablePodName(pod))
			}
		}(ctx, pod)
	}

	// Wait for all pods to be deleted.
	wg.Wait()
}

// loggablePodName returns the "namespace/name" key for the specified pod.
// If the key cannot be computed, "(unknown)" is returned.
// This method is meant to be used for logging purposes only.
func loggablePodName(pod *corev1.Pod) string {
	k, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return "(unknown)"
	}
	return k
}

// loggablePodNameFromCoordinates returns the "namespace/name" key for the pod identified by the specified namespace and name (coordinates).
func loggablePodNameFromCoordinates(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// podsEffectivelyEqual compares two pods, and ignores the pod status, and the resource version
func podsEffectivelyEqual(p1, p2 *corev1.Pod) bool {
	filterForResourceVersion := func(p cmp.Path) bool {
		if p.String() == "ObjectMeta.ResourceVersion" {
			return true
		}
		if p.String() == "Status" {
			return true
		}
		return false
	}

	return cmp.Equal(p1, p2, cmp.FilterPath(filterForResourceVersion, cmp.Ignore()))
}

// borrowed from https://github.com/kubernetes/kubernetes/blob/f64c631cd7aea58d2552ae2038c1225067d30dde/pkg/kubelet/kubelet_pods.go#L944-L953
// running returns true, unless if every status is terminated or waiting, or the status list
// is empty.
func running(podStatus *corev1.PodStatus) bool {
	statuses := podStatus.ContainerStatuses
	for _, status := range statuses {
		if status.State.Terminated == nil && status.State.Waiting == nil {
			return true
		}
	}
	return false
}

func addPodAttributes(ctx context.Context, span trace.Span, pod *corev1.Pod) context.Context {
	return span.WithFields(ctx, log.Fields{
		"uid":       string(pod.GetUID()),
		"namespace": pod.GetNamespace(),
		"name":      pod.GetName(),
		"phase":     string(pod.Status.Phase),
		"reason":    pod.Status.Reason,
	})
}

func (pc *PodController) createOrUpdatePod(ctx context.Context, pod *corev1.Pod) error {

	ctx, span := trace.StartSpan(ctx, "createOrUpdatePod")
	defer span.End()
	addPodAttributes(ctx, span, pod)

	ctx = span.WithFields(ctx, log.Fields{
		"pod":       pod.GetName(),
		"namespace": pod.GetNamespace(),
	})

	// We do this so we don't mutate the pod from the informer cache
	pod = pod.DeepCopy()
	if err := podutils.PopulateEnvironmentVariables(ctx, pod, pc.resourceManager, pc.recorder); err != nil {
		span.SetStatus(err)
		return err
	}

	// We have to use a  different pod that we pass to the provider than the one that gets used in handleProviderError
	// because the provider  may manipulate the pod in a separate goroutine while we were doing work
	podForProvider := pod.DeepCopy()

	// Check if the pod is already known by the provider.
	// NOTE: Some providers return a non-nil error in their GetPod implementation when the pod is not found while some other don't.
	// Hence, we ignore the error and just act upon the pod if it is non-nil (meaning that the provider still knows about the pod).
	if podFromProvider, _ := pc.provider.GetPod(ctx, pod.Namespace, pod.Name); podFromProvider != nil {
		if !podsEqual(podFromProvider, podForProvider) {
			log.G(ctx).Debugf("Pod %s exists, updating pod in provider", podFromProvider.Name)
			if origErr := pc.provider.UpdatePod(ctx, podForProvider); origErr != nil {
				pc.handleProviderError(ctx, span, origErr, pod)
				pc.recorder.Event(pod, corev1.EventTypeWarning, podEventUpdateFailed, origErr.Error())

				return origErr
			}
			log.G(ctx).Info("Updated pod in provider")
			pc.recorder.Event(pod, corev1.EventTypeNormal, podEventUpdateSuccess, "Update pod in provider successfully")

		}
	} else {
		if origErr := pc.provider.CreatePod(ctx, podForProvider); origErr != nil {
			pc.handleProviderError(ctx, span, origErr, pod)
			pc.recorder.Event(pod, corev1.EventTypeWarning, podEventCreateFailed, origErr.Error())
			return origErr
		}
		log.G(ctx).Info("Created pod in provider")
		pc.recorder.Event(pod, corev1.EventTypeNormal, podEventCreateSuccess, "Create pod in provider successfully")
	}
	return nil
}

// podsEqual checks if two pods are equal according to the fields we know that are allowed
// to be modified after startup time.
func podsEqual(pod1, pod2 *corev1.Pod) bool {
	// Pod Update Only Permits update of:
	// - `spec.containers[*].image`
	// - `spec.initContainers[*].image`
	// - `spec.activeDeadlineSeconds`
	// - `spec.tolerations` (only additions to existing tolerations)
	// - `objectmeta.labels`
	// - `objectmeta.annotations`
	// compare the values of the pods to see if the values actually changed

	return cmp.Equal(pod1.Spec.Containers, pod2.Spec.Containers) &&
		cmp.Equal(pod1.Spec.InitContainers, pod2.Spec.InitContainers) &&
		cmp.Equal(pod1.Spec.ActiveDeadlineSeconds, pod2.Spec.ActiveDeadlineSeconds) &&
		cmp.Equal(pod1.Spec.Tolerations, pod2.Spec.Tolerations) &&
		cmp.Equal(pod1.ObjectMeta.Labels, pod2.Labels) &&
		cmp.Equal(pod1.ObjectMeta.Annotations, pod2.Annotations)

}

func deleteGraceTimeEqual(old, new *int64) bool {
	if old == nil && new == nil {
		return true
	}
	if old != nil && new != nil {
		return *old == *new
	}
	return false
}

// podShouldEnqueue checks if two pods equal according according to podsEqual func and DeleteTimeStamp
func podShouldEnqueue(oldPod, newPod *corev1.Pod) bool {
	if !podsEqual(oldPod, newPod) {
		return true
	}
	if !deleteGraceTimeEqual(oldPod.DeletionGracePeriodSeconds, newPod.DeletionGracePeriodSeconds) {
		return true
	}
	if !oldPod.DeletionTimestamp.Equal(newPod.DeletionTimestamp) {
		return true
	}
	return false
}

func (pc *PodController) handleProviderError(ctx context.Context, span trace.Span, origErr error, pod *corev1.Pod) {
	podPhase := corev1.PodPending
	if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
		podPhase = corev1.PodFailed
	}

	pod.ResourceVersion = "" // Blank out resource version to prevent object has been modified error
	pod.Status.Phase = podPhase
	pod.Status.Reason = podStatusReasonProviderFailed
	pod.Status.Message = origErr.Error()

	logger := log.G(ctx).WithFields(log.Fields{
		"podPhase": podPhase,
		"reason":   pod.Status.Reason,
	})

	_, err := pc.client.Pods(pod.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		logger.WithError(err).Warn("Failed to update pod status")
	} else {
		logger.Info("Updated k8s pod status")
	}
	span.SetStatus(origErr)
}

func (pc *PodController) deletePod(ctx context.Context, pod *corev1.Pod) error {
	ctx, span := trace.StartSpan(ctx, "deletePod")
	defer span.End()
	ctx = addPodAttributes(ctx, span, pod)

	err := pc.provider.DeletePod(ctx, pod.DeepCopy())
	if err != nil {
		span.SetStatus(err)
		pc.recorder.Event(pod, corev1.EventTypeWarning, podEventDeleteFailed, err.Error())
		return err
	}
	pc.recorder.Event(pod, corev1.EventTypeNormal, podEventDeleteSuccess, "Delete pod in provider successfully")
	log.G(ctx).Debug("Deleted pod from provider")

	return nil
}

func shouldSkipPodStatusUpdate(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded ||
		pod.Status.Phase == corev1.PodFailed
}

func (pc *PodController) updatePodStatus(ctx context.Context, podFromKubernetes *corev1.Pod, key string) error {
	if shouldSkipPodStatusUpdate(podFromKubernetes) {
		return nil
	}

	ctx, span := trace.StartSpan(ctx, "updatePodStatus")
	defer span.End()
	ctx = addPodAttributes(ctx, span, podFromKubernetes)

	obj, ok := pc.knownPods.Load(key)
	if !ok {
		// This means there was a race and the pod has been deleted from K8s
		return nil
	}
	kPod := obj.(*knownPod)
	kPod.Lock()
	podFromProvider := kPod.lastPodStatusReceivedFromProvider.DeepCopy()
	kPod.Unlock()
	// Pod deleted by provider due some reasons. e.g. a K8s provider, pod created by deployment would be evicted when node is not ready.
	// If we do not delete pod in K8s, deployment would not create a new one.
	if podFromProvider.DeletionTimestamp != nil && podFromKubernetes.DeletionTimestamp == nil {
		deleteOptions := metav1.DeleteOptions{
			GracePeriodSeconds: podFromProvider.DeletionGracePeriodSeconds,
		}
		current := metav1.NewTime(time.Now())
		if podFromProvider.DeletionTimestamp.Before(&current) {
			deleteOptions.GracePeriodSeconds = new(int64)
		}
		// check status here to avoid pod re-created deleted incorrectly. e.g. delete a pod from K8s and re-create it(statefulSet and so on),
		// pod in provider may not delete immediately. so deletionTimestamp is not nil. Then the re-created one would be deleted if we do not check pod status.
		if cmp.Equal(podFromKubernetes.Status, podFromProvider.Status) {
			if err := pc.client.Pods(podFromKubernetes.Namespace).Delete(ctx, podFromKubernetes.Name, deleteOptions); err != nil && !errors.IsNotFound(err) {
				span.SetStatus(err)
				return pkgerrors.Wrap(err, "error while delete pod in kubernetes")
			}
		}
	}

	// We need to do this because the other parts of the pod can be updated elsewhere. Since we're only updating
	// the pod status, and we should be the sole writers of the pod status, we can blind overwrite it. Therefore
	// we need to copy the pod and set ResourceVersion to 0.
	podFromProvider.ResourceVersion = "0"
	if _, err := pc.client.Pods(podFromKubernetes.Namespace).UpdateStatus(ctx, podFromProvider, metav1.UpdateOptions{}); err != nil && !errors.IsNotFound(err) {
		span.SetStatus(err)
		return pkgerrors.Wrap(err, "error while updating pod status in kubernetes")
	}

	log.G(ctx).WithFields(log.Fields{
		"new phase":  string(podFromProvider.Status.Phase),
		"new reason": podFromProvider.Status.Reason,
		"old phase":  string(podFromKubernetes.Status.Phase),
		"old reason": podFromKubernetes.Status.Reason,
	}).Debug("Updated pod status in kubernetes")

	return nil
}

// enqueuePodStatusUpdate updates our pod status map, and marks the pod as dirty in the workqueue. The pod must be DeepCopy'd
// prior to enqueuePodStatusUpdate.
func (pc *PodController) enqueuePodStatusUpdate(ctx context.Context, pod *corev1.Pod) {
	ctx, cancel := context.WithTimeout(ctx, notificationRetryPeriod*queue.MaxRetries)
	defer cancel()

	ctx, span := trace.StartSpan(ctx, "enqueuePodStatusUpdate")
	defer span.End()
	ctx = span.WithField(ctx, "method", "enqueuePodStatusUpdate")

	// TODO (Sargun): Make this asynchronousish. Right now, if we are not cache synced, and we receive notifications
	// from the provider for pods that do not exist yet in our known pods map, we can get into an awkward situation.
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		log.G(ctx).WithError(err).Error("Error getting pod meta namespace key")
		span.SetStatus(err)
		return
	}
	ctx = span.WithField(ctx, "key", key)

	var obj interface{}
	err = wait.PollImmediateUntil(notificationRetryPeriod, func() (bool, error) {
		var ok bool
		obj, ok = pc.knownPods.Load(key)
		if ok {
			return true, nil
		}

		// Blind sync. Partial sync is better than nothing. If this returns false, the poll loop should not be invoked
		// again as it means the context has timed out.
		if !cache.WaitForNamedCacheSync("enqueuePodStatusUpdate", ctx.Done(), pc.podsInformer.Informer().HasSynced) {
			log.G(ctx).Warn("enqueuePodStatusUpdate proceeding with unsynced cache")
		}

		// The only transient error that pod lister returns is not found. The only case where not found
		// should happen, and the pod *actually* exists is the above -- where we haven't been able to finish sync
		// before context times out.
		// The other class of errors is non-transient
		_, err = pc.podsLister.Pods(pod.Namespace).Get(pod.Name)
		if err != nil {
			return false, err
		}

		// err is nil, and therefore the pod exists in k8s, but does not exist in our known pods map. This likely means
		// that we're in some kind of startup synchronization issue where the provider knows about a pod (as it performs
		// recover, that we do not yet know about).
		return false, nil
	}, ctx.Done())

	if err != nil {
		if errors.IsNotFound(err) {
			err = fmt.Errorf("Pod %q not found in pod lister: %w", key, err)
			log.G(ctx).WithError(err).Debug("Not enqueuing pod status update")
		} else {
			log.G(ctx).WithError(err).Warn("Not enqueuing pod status update due to error from pod lister")
		}
		span.SetStatus(err)
		return
	}

	kpod := obj.(*knownPod)
	kpod.Lock()
	if cmp.Equal(kpod.lastPodStatusReceivedFromProvider, pod) {
		kpod.lastPodStatusUpdateSkipped = true
		kpod.Unlock()
		return
	}
	kpod.lastPodStatusUpdateSkipped = false
	kpod.lastPodStatusReceivedFromProvider = pod
	kpod.Unlock()
	pc.syncPodStatusFromProvider.Enqueue(ctx, key)
}

func (pc *PodController) syncPodStatusFromProviderHandler(ctx context.Context, key string) (retErr error) {
	ctx, span := trace.StartSpan(ctx, "syncPodStatusFromProviderHandler")
	defer span.End()

	ctx = span.WithField(ctx, "key", key)
	log.G(ctx).Debug("processing pod status update")
	defer func() {
		span.SetStatus(retErr)
		if retErr != nil {
			log.G(ctx).WithError(retErr).Error("Error processing pod status update")
		}
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return pkgerrors.Wrap(err, "error splitting cache key")
	}

	pod, err := pc.podsLister.Pods(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			log.G(ctx).WithError(err).Debug("Skipping pod status update for pod missing in Kubernetes")
			return nil
		}
		return pkgerrors.Wrap(err, "error looking up pod")
	}

	return pc.updatePodStatus(ctx, pod, key)
}

func (pc *PodController) deletePodsFromKubernetesHandler(ctx context.Context, key string) (retErr error) {
	ctx, span := trace.StartSpan(ctx, "deletePodsFromKubernetesHandler")
	defer span.End()

	uid, metaKey := getUIDAndMetaNamespaceKey(key)
	namespace, name, err := cache.SplitMetaNamespaceKey(metaKey)
	ctx = span.WithFields(ctx, log.Fields{
		"namespace": namespace,
		"name":      name,
	})

	if err != nil {
		// Log the error as a warning, but do not requeue the key as it is invalid.
		log.G(ctx).Warn(pkgerrors.Wrapf(err, "invalid resource key: %q", key))
		span.SetStatus(err)
		return nil
	}

	defer func() {
		if retErr == nil {
			if w, ok := pc.provider.(syncWrapper); ok {
				w._deletePodKey(ctx, key)
			}
		}
	}()

	// If the pod has been deleted from API server, we don't need to do anything.
	k8sPod, err := pc.podsLister.Pods(namespace).Get(name)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		span.SetStatus(err)
		return err
	}
	if string(k8sPod.UID) != uid {
		log.G(ctx).WithField("k8sPodUID", k8sPod.UID).WithField("uid", uid).Warn("Not deleting pod because remote pod has different UID")
		return nil
	}
	if running(&k8sPod.Status) {
		log.G(ctx).Error("Force deleting pod in running state")
	}

	// We don't check with the provider before doing this delete. At this point, even if an outstanding pod status update
	// was in progress,
	deleteOptions := metav1.NewDeleteOptions(0)
	deleteOptions.Preconditions = metav1.NewUIDPreconditions(uid)
	err = pc.client.Pods(namespace).Delete(ctx, name, *deleteOptions)
	if errors.IsNotFound(err) {
		log.G(ctx).Warnf("Not deleting pod because %v", err)
		return nil
	}
	if errors.IsConflict(err) {
		log.G(ctx).Warnf("There was a conflict, maybe trying to delete a Pod that has been recreated: %v", err)
		return nil
	}
	if err != nil {
		span.SetStatus(err)
		return err
	}
	return nil
}

func getUIDAndMetaNamespaceKey(key string) (string, string) {
	idx := strings.LastIndex(key, "/")
	uid := key[idx+1:]
	metaKey := key[:idx]
	return uid, metaKey
}
