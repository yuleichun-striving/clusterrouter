package virtualnode

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"path"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	config "github.com/clusterrouter-io/clusterrouter/cmd/virtualnode-manager/app/config"
	"github.com/clusterrouter-io/clusterrouter/pkg/controllers"
	"github.com/clusterrouter-io/clusterrouter/pkg/plugins"
	"github.com/clusterrouter-io/clusterrouter/pkg/plugins/virtualk8s"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/errdefs"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/log"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/manager"
	"github.com/pkg/errors"
)

const (
	VirtualNodeK8S  = "k8s"
	VirtualEdgeNode = "edgeNode"
)

type VirtualNode struct {
	nodeName          string
	pluginName        string
	podController     *controllers.PodController
	nodeController    *controllers.NodeController
	controllerRunners []controllers.Controller
	pod               kubeinformers.SharedInformerFactory
	scm               kubeinformers.SharedInformerFactory
	mf                kubeinformers.SharedInformerFactory
	cf                kubeinformers.SharedInformerFactory
}

func NewVirtualNode(ctx context.Context, cc *virtualk8s.ClientConfig, c *config.Opts) (*VirtualNode, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	client, err := newClient(c.KubeConfigPath, c.KubeAPIQPS, c.KubeAPIBurst)
	if err != nil {
		return nil, err
	}

	if c.PodSyncWorkers == 0 {
		return nil, errdefs.InvalidInput("pod sync workers must be greater than 0")
	}

	var taint *corev1.Taint
	if !c.DisableTaint {
		var err error
		taint, err = utils.GetTaint(c)
		if err != nil {
			return nil, err
		}
	}

	// Create a shared informer factory for Kubernetes pods in the current namespace (if specified) and scheduled to the current node.
	podInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(
		client,
		c.InformerResyncPeriod,
		kubeinformers.WithNamespace(c.KubeNamespace),
		kubeinformers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", c.NodeName).String()
		}))
	podInformer := podInformerFactory.Core().V1().Pods()

	// Create another shared informer factory for Kubernetes secrets and configmaps (not subject to any selectors).
	scmInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(client, c.InformerResyncPeriod)
	// Create a secret informer and a config map informer so we can pass their listers to the resource manager.
	secretInformer := scmInformerFactory.Core().V1().Secrets()
	configMapInformer := scmInformerFactory.Core().V1().ConfigMaps()
	serviceInformer := scmInformerFactory.Core().V1().Services()

	rm, err := manager.NewResourceManager(podInformer.Lister(), secretInformer.Lister(), configMapInformer.Lister(), serviceInformer.Lister())
	if err != nil {
		return nil, errors.Wrap(err, "could not create resource manager")
	}

	/*	// Start the informers now, so the provider will get a functional resource
		// manager.
		podInformerFactory.Start(ctx.Done())
		scmInformerFactory.Start(ctx.Done())*/

	initConfig := plugins.InitConfig{
		ConfigPath:        c.KubeConfigPath,
		NodeName:          c.NodeName,
		OperatingSystem:   c.OperatingSystem,
		ResourceManager:   rm,
		DaemonPort:        int32(c.ListenPort),
		InternalIP:        os.Getenv("VKUBELET_POD_IP"),
		KubeClusterDomain: c.KubeClusterDomain,
	}

	var p plugins.Provider
	switch c.Provider {
	case VirtualNodeK8S:
		p, err = virtualk8s.NewVirtualK8S(initConfig, cc, "", true, c)
		if err != nil {
			return nil, errors.Wrapf(err, "error initializing provider %s", c.Provider)
		}
	case VirtualEdgeNode:
		return nil, fmt.Errorf("virtualEdgeNode not ready!")
	}

	ctx = log.WithLogger(ctx, log.G(ctx).WithFields(log.Fields{
		"provider":         c.Provider,
		"operatingSystem":  c.OperatingSystem,
		"node":             c.NodeName,
		"watchedNamespace": c.KubeNamespace,
	}))

	var leaseClient v1.LeaseInterface
	if c.EnableNodeLease {
		leaseClient = client.CoordinationV1().Leases(corev1.NamespaceNodeLease)
	}
	nodeProvider, ok := p.(plugins.NodeProvider)
	if !ok {
		return nil, fmt.Errorf("NodeProvider not ready!")
	}
	pNode := utils.NodeFromProvider(ctx, c.NodeName, taint, p, c.Version)
	nodeRunner, err := controllers.NewNodeController(
		nodeProvider,
		pNode,
		client.CoreV1().Nodes(),
		controllers.WithNodeEnableLeaseV1(leaseClient, controllers.DefaultLeaseDuration),
		controllers.WithNodeStatusUpdateErrorHandler(func(ctx context.Context, err error) error {
			if !k8serrors.IsNotFound(err) {
				return err
			}

			log.G(ctx).Debug("node not found")
			newNode := pNode.DeepCopy()
			newNode.ResourceVersion = ""
			_, err = client.CoreV1().Nodes().Create(ctx, newNode, metav1.CreateOptions{})
			if err != nil {
				return err
			}
			log.G(ctx).Debug("created new node")
			return nil
		}),
	)
	if err != nil {
		log.G(ctx).Fatal(err)
	}

	eb := record.NewBroadcaster()
	eb.StartLogging(log.G(ctx).Infof)
	eb.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: client.CoreV1().Events(c.KubeNamespace)})

	pc, err := controllers.NewPodController(controllers.PodControllerConfig{
		PodClient:                            client.CoreV1(),
		PodInformer:                          podInformer,
		EventRecorder:                        eb.NewRecorder(scheme.Scheme, corev1.EventSource{Component: path.Join(pNode.Name, "pod-controller")}),
		Provider:                             p,
		SecretInformer:                       secretInformer,
		ConfigMapInformer:                    configMapInformer,
		ServiceInformer:                      serviceInformer,
		SyncPodsFromKubernetesRateLimiter:    rateLimiter(),
		DeletePodsFromKubernetesRateLimiter:  rateLimiter(),
		SyncPodStatusFromProviderRateLimiter: rateLimiter(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "error setting up pod controller")
	}

	controllerRunners, mf, cf, err := ControllerRunners(ctx, c.NodeName, c, cc)
	if err != nil {
		return nil, errors.Wrap(err, "error get controllerRunners")
	}

	virtualNode := &VirtualNode{
		nodeName:          c.NodeName,
		pluginName:        c.Provider,
		podController:     pc,
		nodeController:    nodeRunner,
		controllerRunners: controllerRunners,
		pod:               podInformerFactory,
		scm:               scmInformerFactory,
		mf:                mf,
		cf:                cf,
	}

	return virtualNode, nil

	/*	cancelHTTP, err := setupHTTPServer(ctx, p, apiConfig)
		if err != nil {
			return err
		}
		defer cancelHTTP()*/
}

// RunController starts controllers for objects needed to be synced
func ControllerRunners(ctx context.Context, hostIP string, opts *config.Opts, cc *virtualk8s.ClientConfig) ([]controllers.Controller, kubeinformers.SharedInformerFactory,
	kubeinformers.SharedInformerFactory, error) {

	client, err := utils.NewClientFromByte(cc.ClientKubeConfig, func(config *rest.Config) {
		config.QPS = float32(cc.KubeClientQPS)
		config.Burst = cc.KubeClientBurst
		// Set config for clientConfig
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not build clientset for cluster: %v", err)
	}

	// master config, maybe a real node or a pod
	master, err := utils.NewClient(opts.KubeConfigPath, func(config *rest.Config) {
		config.QPS = float32(opts.KubeAPIQPS)
		config.Burst = int(opts.KubeAPIBurst)
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not build clientset for cluster: %v", err)
	}

	masterInformer := kubeinformers.NewSharedInformerFactory(master, 0)
	if masterInformer == nil {
		return nil, nil, nil, fmt.Errorf("could not build masterInformer")
	}
	clientInformer := kubeinformers.NewSharedInformerFactory(client, 1*time.Minute)
	if clientInformer == nil {
		return nil, nil, nil, fmt.Errorf("could not build clientInformer")
	}

	runningControllers := []controllers.Controller{buildCommonControllers(client, masterInformer, clientInformer)}

	pvCtrl := controllers.NewPVController(master, client, masterInformer, clientInformer, hostIP)
	runningControllers = append(runningControllers, pvCtrl)
	serviceCtrl := controllers.NewServiceController(master, client, masterInformer, clientInformer)
	runningControllers = append(runningControllers, serviceCtrl)

/*	masterInformer.Start(ctx.Done())
	clientInformer.Start(ctx.Done())*/

	return runningControllers, masterInformer, clientInformer, nil
}

func (v *VirtualNode) Run(ctx context.Context, c *config.Opts) error {
	go func() {
		if err := v.podController.Run(ctx, c.PodSyncWorkers); err != nil && errors.Cause(err) != context.Canceled {
			log.G(ctx).Fatal(err)
		}
	}()

	if c.StartupTimeout > 0 {
		// If there is a startup timeout, it does two things:
		// 1. It causes the VK to shutdown if we haven't gotten into an operational state in a time period
		// 2. It prevents node advertisement from happening until we're in an operational state
		err := waitFor(ctx, c.StartupTimeout, v.podController.Ready())
		if err != nil {
			return err
		}
	}

	go func() {
		if err := v.nodeController.Run(ctx); err != nil {
			log.G(ctx).Fatal(err)
		}
	}()

	v.pod.Start(ctx.Done())
	v.scm.Start(ctx.Done())

	for _, ctrl := range v.controllerRunners {
		go ctrl.Run(1, ctx.Done())
	}

	v.mf.Start(ctx.Done())
	v.cf.Start(ctx.Done())

	klog.Info("Initializing")

	<-ctx.Done()
	return nil
}

func (v *VirtualNode) Shutdown() {

}

func buildCommonControllers(client kubernetes.Interface, masterInformer,
	clientInformer kubeinformers.SharedInformerFactory) controllers.Controller {

	configMapRateLimiter := workqueue.NewItemExponentialFailureRateLimiter(time.Second, 30*time.Second)
	secretRateLimiter := workqueue.NewItemExponentialFailureRateLimiter(time.Second, 30*time.Second)

	return controllers.NewCommonController(client, masterInformer, clientInformer, configMapRateLimiter, secretRateLimiter)
}

func rateLimiter() workqueue.RateLimiter {
	return workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
		// 100 qps, 1000 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(100), 1000)},
	)
}

func waitFor(ctx context.Context, time time.Duration, ready <-chan struct{}) error {
	ctx, cancel := context.WithTimeout(ctx, time)
	defer cancel()

	// Wait for the VK / PC close the the ready channel, or time out and return
	log.G(ctx).Info("Waiting for pod controller / VK to be ready")

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "Error while starting up VK")
	}
}

func newClient(configPath string, qps, burst int32) (*kubernetes.Clientset, error) {
	var config *rest.Config

	// Check if the kubeConfig file exists.
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		// Get the kubeconfig from the filepath.
		config, err = clientcmd.BuildConfigFromFlags("", configPath)
		if err != nil {
			return nil, errors.Wrap(err, "error building client config")
		}
	} else {
		// Set to in-cluster config.
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, errors.Wrap(err, "error building in cluster config")
		}
	}

	if qps != 0 {
		config.QPS = float32(qps)
	}

	if burst != 0 {
		config.Burst = int(burst)
	}

	if masterURI := os.Getenv("MASTER_URI"); masterURI != "" {
		config.Host = masterURI
	}

	return kubernetes.NewForConfig(config)
}
