package plugins

import "github.com/clusterrouter-io/clusterrouter/pkg/utils/manager"

// InitConfig is the config passed to initialize a registered provider.
type InitConfig struct {
	ConfigPath        string
	NodeName          string
	OperatingSystem   string
	InternalIP        string
	DaemonPort        int32
	KubeClusterDomain string
	ResourceManager   *manager.ResourceManager
}

type InitFunc func(InitConfig) (Provider, error)
