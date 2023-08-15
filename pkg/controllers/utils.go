package controllers

import (
	"context"
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"

	"github.com/clusterrouter-io/clusterrouter/pkg/utils"
)

func ensureNamespace(ns string, client kubernetes.Interface, nsLister corelisters.NamespaceLister) error {
	_, err := nsLister.Get(ns)
	if err == nil {
		return nil
	}
	if !apierrs.IsNotFound(err) {
		return err
	}
	if _, err = client.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		if !apierrs.IsAlreadyExists(err) {
			return err
		}
		return nil
	}

	return err
}

func filterPVC(pvcInSub *v1.PersistentVolumeClaim, hostIP string) error {
	labelSelector := pvcInSub.Spec.Selector.DeepCopy()
	pvcInSub.Spec.Selector = nil
	utils.TrimObjectMeta(&pvcInSub.ObjectMeta)
	SetObjectGlobal(&pvcInSub.ObjectMeta)
	if labelSelector != nil {
		labelStr, err := json.Marshal(labelSelector)
		if err != nil {
			return err
		}
		pvcInSub.Annotations["labelSelector"] = string(labelStr)
	}
	if len(pvcInSub.Annotations[utils.SelectedNodeKey]) != 0 {
		pvcInSub.Annotations[utils.SelectedNodeKey] = hostIP
	}
	return nil
}

func filterPV(pvInSub *v1.PersistentVolume, hostIP string) {
	utils.TrimObjectMeta(&pvInSub.ObjectMeta)
	if pvInSub.Annotations == nil {
		pvInSub.Annotations = make(map[string]string)
	}
	if pvInSub.Spec.NodeAffinity == nil {
		return
	}
	if pvInSub.Spec.NodeAffinity.Required == nil {
		return
	}
	terms := pvInSub.Spec.NodeAffinity.Required.NodeSelectorTerms
	for k, v := range pvInSub.Spec.NodeAffinity.Required.NodeSelectorTerms {
		mf := v.MatchFields
		me := v.MatchExpressions
		for k, val := range v.MatchFields {
			if val.Key == utils.HostNameKey || val.Key == utils.BetaHostNameKey {
				val.Values = []string{hostIP}
			}
			mf[k] = val
		}
		for k, val := range v.MatchExpressions {
			if val.Key == utils.HostNameKey || val.Key == utils.BetaHostNameKey {
				val.Values = []string{hostIP}
			}
			me[k] = val
		}
		terms[k].MatchFields = mf
		terms[k].MatchExpressions = me
	}
	pvInSub.Spec.NodeAffinity.Required.NodeSelectorTerms = terms
	return
}

func filterCommon(meta *metav1.ObjectMeta) error {
	utils.TrimObjectMeta(meta)
	SetObjectGlobal(meta)
	return nil
}

func filterService(serviceInSub *v1.Service) error {
	labelSelector := serviceInSub.Spec.Selector
	serviceInSub.Spec.Selector = nil
	if serviceInSub.Spec.ClusterIP != "None" {
		serviceInSub.Spec.ClusterIP = ""
	}
	if len(serviceInSub.Spec.ClusterIPs) > 0 {
		serviceInSub.Spec.ClusterIPs = []string{}
	}
	utils.TrimObjectMeta(&serviceInSub.ObjectMeta)
	SetObjectGlobal(&serviceInSub.ObjectMeta)
	if labelSelector == nil {
		return nil
	}
	labelStr, err := json.Marshal(labelSelector)
	if err != nil {
		return err
	}
	serviceInSub.Annotations["labelSelector"] = string(labelStr)
	return nil
}

// CheckGlobalLabelEqual checks if two objects both has the global label
func CheckGlobalLabelEqual(obj, clone *metav1.ObjectMeta) bool {
	oldGlobal := IsObjectGlobal(obj)
	if !oldGlobal {
		return false
	}
	newGlobal := IsObjectGlobal(clone)
	if !newGlobal {
		return false
	}
	return true
}

// IsObjectGlobal return if an object is global
func IsObjectGlobal(obj *metav1.ObjectMeta) bool {
	if obj.Annotations == nil {
		return false
	}

	if obj.Annotations[utils.GlobalLabel] == "true" {
		return true
	}

	return false
}

// SetObjectGlobal add global annotation to an object
func SetObjectGlobal(obj *metav1.ObjectMeta) {
	if obj.Annotations == nil {
		obj.Annotations = map[string]string{}
	}
	obj.Annotations[utils.GlobalLabel] = "true"
}
