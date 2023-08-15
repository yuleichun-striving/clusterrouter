package utils

import (
	"context"
	"github.com/clusterrouter-io/clusterrouter/cmd/virtualnode-manager/app/config"
	"github.com/clusterrouter-io/clusterrouter/pkg/plugins"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/errdefs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

const osLabel = "beta.kubernetes.io/os"

// NodeFromProvider builds a kubernetes node object from a provider
// This is a temporary solution until node stuff actually split off from the provider interface itself.
func NodeFromProvider(ctx context.Context, name string, taint *corev1.Taint, p plugins.Provider, version string) *corev1.Node {
	taints := make([]corev1.Taint, 0)

	if taint != nil {
		taints = append(taints, *taint)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"type":                   "cluster-router",
				"kubernetes.io/role":     "agent",
				"kubernetes.io/hostname": name,
			},
		},
		Spec: corev1.NodeSpec{
			Taints: taints,
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{
				Architecture:   "amd64",
				KubeletVersion: version,
			},
		},
	}

	p.ConfigureNode(ctx, node)
	if _, ok := node.ObjectMeta.Labels[osLabel]; !ok {
		node.ObjectMeta.Labels[osLabel] = strings.ToLower(node.Status.NodeInfo.OperatingSystem)
	}
	return node
}

// getTaint creates a taint using the provided key/value.
// Taint effect is read from the environment
// The taint key/value may be overwritten by the environment.
func GetTaint(o *config.Opts) (*corev1.Taint, error) {
	if o.TaintValue == "" {
		o.TaintValue = o.Provider
	}

	var effect corev1.TaintEffect
	switch o.TaintEffect {
	case "NoSchedule":
		effect = corev1.TaintEffectNoSchedule
	case "NoExecute":
		effect = corev1.TaintEffectNoExecute
	case "PreferNoSchedule":
		effect = corev1.TaintEffectPreferNoSchedule
	default:
		return nil, errdefs.InvalidInputf("taint effect %q is not supported", o.TaintEffect)
	}

	return &corev1.Taint{
		Key:    o.TaintKey,
		Value:  o.TaintValue,
		Effect: effect,
	}, nil
}
