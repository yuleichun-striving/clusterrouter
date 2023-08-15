package options

import (
	"github.com/clusterrouter-io/clusterrouter/cmd/virtualnode-manager/app/config"
	crdclientset "github.com/clusterrouter-io/clusterrouter/pkg/generated/clientset/versioned"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	cliflag "k8s.io/component-base/cli/flag"
	componentbaseconfig "k8s.io/component-base/config"
	componentbaseconfigv1alpha1 "k8s.io/component-base/config/v1alpha1"
	"os"
	"strings"
)

const (
	VirtualNodeManagerUserAgent = "virtual-node-manager"
)

var (
	buildVersion         = "N/A"
	buildTime            = "N/A"
	k8sVersion           = "v1.14.3"
	numberOfWorkers      = 50
	ignoreLabels         = ""
	enableControllers    = ""
	enableServiceAccount = true
	providerName         = "k8s"
)

type Options struct {
	LeaderElection componentbaseconfig.LeaderElectionConfiguration

	WorkerNumber int

	Opts *config.Opts
}

func NewVirtualNodeOptions() (*Options, error) {
	var leaderElection componentbaseconfigv1alpha1.LeaderElectionConfiguration
	componentbaseconfigv1alpha1.RecommendedDefaultLeaderElectionConfiguration(&leaderElection)

	leaderElection.ResourceName = "virtualnode-manager"
	leaderElection.ResourceNamespace = "virtualnode-system"
	leaderElection.ResourceLock = resourcelock.LeasesResourceLock

	var options Options
	if err := componentbaseconfigv1alpha1.Convert_v1alpha1_LeaderElectionConfiguration_To_config_LeaderElectionConfiguration(&leaderElection, &options.LeaderElection, nil); err != nil {
		return nil, err
	}

	options.WorkerNumber = 1

	o, err := config.FromEnv()
	if err != nil {
		panic(err)
	}
	o.Provider = providerName
	o.PodSyncWorkers = numberOfWorkers
	o.Version = strings.Join([]string{k8sVersion, providerName, buildVersion}, "-")

	options.Opts = o

	return &options, nil
}

func (o *Options) Config() (*config.Config, error) {
	kubeconfig, err := clientcmd.BuildConfigFromFlags("", o.Opts.KubeConfigPath)
	if err != nil {
		return nil, err
	}

	crdclient, err := crdclientset.NewForConfig(restclient.AddUserAgent(kubeconfig, VirtualNodeManagerUserAgent))
	if err != nil {
		return nil, err
	}

	return &config.Config{
		WorkerNumber:   o.WorkerNumber,
		LeaderElection: o.LeaderElection,
		KubeConfig:     kubeconfig,
		CRDClient:      crdclient,
		Opts:           o.Opts,
	}, nil
}

func (o *Options) Flags() cliflag.NamedFlagSets {
	var fss cliflag.NamedFlagSets

	fs := fss.FlagSet("misc")
	fs.StringVar(&o.Opts.KubeConfigPath, "kubeconfig", o.Opts.KubeConfigPath, "kube config file to use for connecting to the Kubernetes API server")
	fs.StringVar(&o.Opts.KubeNamespace, "namespace", o.Opts.KubeNamespace, "kubernetes namespace (default is 'all')")
	fs.StringVar(&o.Opts.KubeClusterDomain, "cluster-domain", o.Opts.KubeClusterDomain, "kubernetes cluster-domain (default is 'cluster.local')")
	fs.StringVar(&o.Opts.NodeName, "nodename", o.Opts.NodeName, "kubernetes node name")
	fs.StringVar(&o.Opts.OperatingSystem, "os", o.Opts.OperatingSystem, "Operating System (Linux/Windows)")
	fs.StringVar(&o.Opts.Provider, "provider", o.Opts.Provider, "cloud provider")
	fs.StringVar(&o.Opts.ProviderConfigPath, "provider-config", o.Opts.ProviderConfigPath, "cloud provider configuration file")
	fs.StringVar(&o.Opts.MetricsAddr, "metrics-addr", o.Opts.MetricsAddr, "address to listen for metrics/stats requests")

	//fs.StringVar(&o.Opts.TaintKey, "taint", o.Opts.TaintKey, "Set node taint key")
	//fs.BoolVar(&o.Opts.DisableTaint, "disable-taint", o.Opts.DisableTaint, "disable the cluster-router node taint")
	fs.MarkDeprecated("taint", "Taint key should now be configured using the VK_TAINT_KEY environment variable")

	fs.IntVar(&o.Opts.PodSyncWorkers, "pod-sync-workers", o.Opts.PodSyncWorkers, `set the number of pod synchronization workers`)
	fs.BoolVar(&o.Opts.EnableNodeLease, "enable-node-lease", o.Opts.EnableNodeLease, `use node leases (1.13) for node heartbeats`)

	fs.DurationVar(&o.Opts.InformerResyncPeriod, "full-resync-period", o.Opts.InformerResyncPeriod, "how often to perform a full resync of pods between kubernetes and the provider")
	fs.DurationVar(&o.Opts.StartupTimeout, "startup-timeout", o.Opts.StartupTimeout, "How long to wait for the cluster-router to start")

	fs.Int32Var(&o.Opts.KubeAPIQPS, "kube-api-qps", o.Opts.KubeAPIQPS,
		"kubeAPIQPS is the QPS to use while talking with kubernetes apiserver")
	fs.Int32Var(&o.Opts.KubeAPIBurst, "kube-api-burst", o.Opts.KubeAPIBurst,
		"kubeAPIBurst is the burst to allow while talking with kubernetes apiserver")

	fs.StringVar(&o.Opts.ClientCACert, "client-verify-ca", os.Getenv("APISERVER_CA_CERT_LOCATION"), "CA cert to use to verify client requests")
	fs.BoolVar(&o.Opts.AllowUnauthenticatedClients, "no-verify-clients", false, "Do not require client certificate validation")
	fs.BoolVar(&o.LeaderElection.LeaderElect, "leader-elect", o.LeaderElection.LeaderElect, ""+
		"Start a leader election client and gain leadership before "+
		"executing the main loop. Enable this when running replicated "+
		"components for high availability.")
	return fss
}
