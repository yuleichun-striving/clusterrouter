package common

import (
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
)

type ProviderNode struct {
	sync.Mutex
	*corev1.Node
}

// AddResource add resource to the node
func (n *ProviderNode) AddResource(resource *Resource) error {
	if n.Node == nil {
		return fmt.Errorf("ProviderNode node has not init")
	}
	n.Lock()
	defer n.Unlock()
	vkResource := ConvertResource(n.Status.Capacity)

	vkResource.Add(resource)
	vkResource.SetCapacityToNode(n.Node)
	return nil
}

// SubResource sub resource from the node
func (n *ProviderNode) SubResource(resource *Resource) error {
	if n.Node == nil {
		return fmt.Errorf("ProviderNode node has not init")
	}
	n.Lock()
	defer n.Unlock()
	vkResource := ConvertResource(n.Status.Capacity)

	vkResource.Sub(resource)
	vkResource.SetCapacityToNode(n.Node)
	return nil
}

// DeepCopy deepcopy node with lock, to avoid concurrent read-write
func (n *ProviderNode) DeepCopy() *corev1.Node {
	n.Lock()
	node := n.Node.DeepCopy()
	n.Unlock()
	return node
}
