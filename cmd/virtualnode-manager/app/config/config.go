package config

import (
	"os"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	restclient "k8s.io/client-go/rest"
	componentbaseconfig "k8s.io/component-base/config"

	crdclientset "github.com/clusterrouter-io/clusterrouter/pkg/generated/clientset/versioned"
	"github.com/pkg/errors"
)

// Defaults for root command options
const (
	DefaultNodeName             = "virtual-Node"
	DefaultOperatingSystem      = "Linux"
	DefaultInformerResyncPeriod = 1 * time.Minute
	DefaultMetricsAddr          = ""
	DefaultListenPort           = 10250 // TODO(cpuguy83)(VK1.0): Change this to an addr instead of just a port.. we should not be listening on all interfaces.
	DefaultPodSyncWorkers       = 10
	DefaultKubeNamespace        = corev1.NamespaceAll
	DefaultKubeClusterDomain    = "cluster.local"

	DefaultTaintEffect           = string(corev1.TaintEffectNoSchedule)
	DefaultTaintKey              = "virtual-node.io/plugin"
	DefaultStreamIdleTimeout     = 4 * time.Hour
	DefaultStreamCreationTimeout = 30 * time.Second
)

type Config struct {
	KubeConfig     *restclient.Config
	CRDClient      *crdclientset.Clientset
	WorkerNumber   int
	LeaderElection componentbaseconfig.LeaderElectionConfiguration
	Opts           *Opts
}

type Opts struct {
	// Path to the kubeconfig to use to connect to the Kubernetes API server.
	KubeConfigPath string
	// Namespace to watch for pods and other resources
	KubeNamespace string
	// Domain suffix to append to search domains for the pods created by cluster-router
	KubeClusterDomain string

	// Sets the port to listen for requests from the Kubernetes API server
	ListenPort int32

	// Node name to use when creating a node in Kubernetes
	NodeName string

	// Operating system to run pods for
	OperatingSystem string

	Provider           string
	ProviderConfigPath string

	TaintKey     string
	TaintEffect  string
	TaintValue   string
	DisableTaint bool

	MetricsAddr string

	// Only trust clients with tls certs signed by the provided CA
	ClientCACert string
	// Do not require client tls verification
	AllowUnauthenticatedClients bool

	// Number of workers to use to handle pod notifications
	PodSyncWorkers       int
	InformerResyncPeriod time.Duration

	// Use node leases when supported by Kubernetes (instead of node status updates)
	EnableNodeLease bool

	// Startup Timeout is how long to wait for the kubelet to start
	StartupTimeout time.Duration
	// StreamIdleTimeout is the maximum time a streaming connection
	// can be idle before the connection is automatically closed.
	StreamIdleTimeout time.Duration
	// StreamCreationTimeout is the maximum time for streaming connection
	StreamCreationTimeout time.Duration

	// KubeAPIQPS is the QPS to use while talking with kubernetes apiserver
	KubeAPIQPS int32
	// KubeAPIBurst is the burst to allow while talking with kubernetes apiserver
	KubeAPIBurst int32

	/*	// SyncPodsFromKubernetesRateLimiter defines the rate limit for the SyncPodsFromKubernetes queue
		SyncPodsFromKubernetesRateLimiter workqueue.RateLimiter
		// DeletePodsFromKubernetesRateLimiter defines the rate limit for the DeletePodsFromKubernetesRateLimiter queue
		DeletePodsFromKubernetesRateLimiter workqueue.RateLimiter
		// SyncPodStatusFromProviderRateLimiter defines the rate limit for the SyncPodStatusFromProviderRateLimiter queue
		SyncPodStatusFromProviderRateLimiter workqueue.RateLimiter
	*/
	Version string
}

// FromEnv sets default options for unset values on the passed in option struct.
// Fields tht are already set will not be modified.
func FromEnv() (*Opts, error) {
	o := &Opts{}
	setDefaults(o)

	o.NodeName = getEnv("DEFAULTNODE_NAME", o.NodeName)

	if kp := os.Getenv("KUBELET_PORT"); kp != "" {
		p, err := strconv.Atoi(kp)
		if err != nil {
			return o, errors.Wrap(err, "error parsing KUBELET_PORT environment variable")
		}
		o.ListenPort = int32(p)
	}

	o.KubeConfigPath = os.Getenv("KUBECONFIG")
	/*	if o.KubeConfigPath == "" {
		home, _ := homedir.Dir()
		if home != "" {
			o.KubeConfigPath = filepath.Join(home, ".kube", "config")
		}
	}*/

	o.TaintKey = getEnv("VKUBELET_TAINT_KEY", o.TaintKey)
	o.TaintValue = getEnv("VKUBELET_TAINT_VALUE", o.TaintValue)
	o.TaintEffect = getEnv("VKUBELET_TAINT_EFFECT", o.TaintEffect)

	return o, nil
}

func New() *Opts {
	o := &Opts{}
	setDefaults(o)
	return o
}

func setDefaults(o *Opts) {
	o.OperatingSystem = DefaultOperatingSystem
	o.NodeName = DefaultNodeName
	o.TaintKey = DefaultTaintKey
	o.TaintEffect = DefaultTaintEffect
	o.KubeNamespace = DefaultKubeNamespace
	o.PodSyncWorkers = DefaultPodSyncWorkers
	o.ListenPort = DefaultListenPort
	o.MetricsAddr = DefaultMetricsAddr
	o.InformerResyncPeriod = DefaultInformerResyncPeriod
	o.KubeClusterDomain = DefaultKubeClusterDomain
	o.StreamIdleTimeout = DefaultStreamIdleTimeout
	o.StreamCreationTimeout = DefaultStreamCreationTimeout
	o.EnableNodeLease = true
}

func getEnv(key, defaultValue string) string {
	value, found := os.LookupEnv(key)
	if found {
		return value
	}
	return defaultValue
}
