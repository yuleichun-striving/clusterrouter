package virtualk8s

import (
	"context"
	"fmt"
	"github.com/clusterrouter-io/clusterrouter/pkg/controllers"
	"github.com/clusterrouter-io/clusterrouter/pkg/plugins"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils"
	"github.com/clusterrouter-io/clusterrouter/pkg/utils/errdefs"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"
	"reflect"
	"strings"
	"time"
)

var _ plugins.PodLifecycleHandler = &VirtualK8S{}
var _ plugins.PodNotifier = &VirtualK8S{}
var _ plugins.NodeProvider = &VirtualK8S{}

const RooTCAConfigMapName = "kube-root-ca.crt"
const SATokenPrefix = "kube-api-access"
const MasterRooTCAName = "master-root-ca.crt"

// CreatePod takes a Kubernetes Pod and deploys it within the provider.
func (v *VirtualK8S) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	if pod.Namespace == "kube-system" {
		return nil
	}
	basicPod := utils.TrimPod(pod, v.ignoreLabels)
	klog.V(3).Infof("Creating pod %v/%+v", pod.Namespace, pod.Name)
	if _, err := v.clientCache.nsLister.Get(pod.Namespace); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		klog.Infof("Namespace %s does not exist for pod %s, creating it", pod.Namespace, pod.Name)
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: pod.Namespace,
			},
		}
		if _, createErr := v.client.CoreV1().Namespaces().Create(ctx, ns,
			metav1.CreateOptions{}); createErr != nil && errors.IsAlreadyExists(createErr) {
			klog.Infof("Namespace %s create failed error: %v", pod.Namespace, createErr)
			return err
		}
	}
	secretNames := getSecrets(pod)
	configMaps := getConfigmaps(pod)
	pvcs := getPVCs(pod)
	go wait.PollImmediate(500*time.Millisecond, 10*time.Minute, func() (bool, error) {
		klog.V(4).Info("Trying to creating base dependent")
		if err := v.createConfigMaps(ctx, configMaps, pod.Namespace); err != nil {
			klog.Error(err)
			return false, nil
		}
		klog.Infof("Create configmaps %v of %v/%v success", configMaps, pod.Namespace, pod.Name)
		if err := v.createPVCs(ctx, pvcs, pod.Namespace); err != nil {
			klog.Error(err)
			return false, nil
		}
		klog.Infof("Create pvc %v of %v/%v success", pvcs, pod.Namespace, pod.Name)
		return true, nil
	})
	var err error
	wait.PollImmediate(100*time.Millisecond, 1*time.Second, func() (bool, error) {
		klog.V(4).Info("Trying to creating secret and service account")
		if err = v.createSecrets(ctx, secretNames, pod.Namespace); err != nil {
			klog.Error(err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("create secrets failed: %v", err)
	}

	v.convertAuth(ctx, pod)

	klog.V(6).Infof("Creating pod %+v", pod)
	_, err = v.client.CoreV1().Pods(pod.Namespace).Create(ctx, basicPod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("could not create pod: %v", err)
	}
	klog.V(3).Infof("Create pod %v/%+v success", pod.Namespace, pod.Name)
	return nil
}

// UpdatePod takes a Kubernetes Pod and updates it within the provider.
func (v *VirtualK8S) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	if pod.Namespace == "kube-system" {
		return nil
	}
	klog.V(3).Infof("Updating pod %v/%+v", pod.Namespace, pod.Name)
	currentPod, err := v.GetPod(ctx, pod.Namespace, pod.Name)
	if err != nil {
		return fmt.Errorf("could not get current pod")
	}
	if !utils.IsVirtualPod(pod) {
		klog.Info("Pod is not created by vk, ignore")
		return nil
	}
	//tripped ignore labels which recoverd in currentPod
	utils.TrimLabels(currentPod.ObjectMeta.Labels, v.ignoreLabels)
	podCopy := currentPod.DeepCopy()
	// util.GetUpdatedPod update PodCopy container image, annotations, labels.
	// recover toleration, affinity, tripped ignore labels.
	utils.GetUpdatedPod(podCopy, pod, v.ignoreLabels)
	if reflect.DeepEqual(currentPod.Spec, podCopy.Spec) &&
		reflect.DeepEqual(currentPod.Annotations, podCopy.Annotations) &&
		reflect.DeepEqual(currentPod.Labels, podCopy.Labels) {
		return nil
	}
	_, err = v.client.CoreV1().Pods(pod.Namespace).Update(ctx, podCopy, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("could not update pod: %v", err)
	}
	klog.V(3).Infof("Update pod %v/%+v success ", pod.Namespace, pod.Name)
	return nil
}

// DeletePod takes a Kubernetes Pod and deletes it from the provider.
func (v *VirtualK8S) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	if pod.Namespace == "kube-system" {
		return nil
	}
	klog.V(3).Infof("Deleting pod %v/%+v", pod.Namespace, pod.Name)

	if !utils.IsVirtualPod(pod) {
		klog.Info("Pod is not create by vk, ignore")
		return nil
	}

	opts := &metav1.DeleteOptions{
		GracePeriodSeconds: new(int64), // 0
	}
	if pod.DeletionGracePeriodSeconds != nil {
		opts.GracePeriodSeconds = pod.DeletionGracePeriodSeconds
	}

	err := v.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, *opts)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Infof("Tried to delete pod %s/%s, but it did not exist in the cluster", pod.Namespace, pod.Name)
			return nil
		}
		return fmt.Errorf("could not delete pod: %v", err)
	}
	klog.V(3).Infof("Delete pod %v/%+v success", pod.Namespace, pod.Name)
	return nil
}

// GetPod retrieves a pod by name from the provider (can be cached).
// The Pod returned is expected to be immutable, and may be accessed
// concurrently outside of the calling goroutine. Therefore it is recommended
// to return a version after DeepCopy.
func (v *VirtualK8S) GetPod(ctx context.Context, namespace string, name string) (*corev1.Pod, error) {
	pod, err := v.clientCache.podLister.Pods(namespace).Get(name)
	if err != nil {
		klog.Error(err)
		if errors.IsNotFound(err) {
			return nil, errdefs.AsNotFound(err)
		}
		return nil, fmt.Errorf("could not get pod %s/%s: %v", namespace, name, err)
	}
	podCopy := pod.DeepCopy()
	utils.RecoverLabels(podCopy.Labels, podCopy.Annotations)
	return podCopy, nil
}

// GetPodStatus retrieves the status of a pod by name from the provider.
// The PodStatus returned is expected to be immutable, and may be accessed
// concurrently outside of the calling goroutine. Therefore it is recommended
// to return a version after DeepCopy.
func (v *VirtualK8S) GetPodStatus(ctx context.Context, namespace string, name string) (*corev1.PodStatus, error) {
	pod, err := v.clientCache.podLister.Pods(namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("could not get pod %s/%s: %v", namespace, name, err)
	}
	return pod.Status.DeepCopy(), nil
}

// GetPods retrieves a list of all pods running on the provider (can be cached).
// The Pods returned are expected to be immutable, and may be accessed
// concurrently outside of the calling goroutine. Therefore it is recommended
// to return a version after DeepCopy.
func (v *VirtualK8S) GetPods(_ context.Context) ([]*corev1.Pod, error) {
	set := labels.Set{utils.VirtualPodLabel: "true"}
	pods, err := v.clientCache.podLister.List(labels.SelectorFromSet(set))
	if err != nil {
		return nil, fmt.Errorf("could not list pods: %v", err)
	}

	podRefs := []*corev1.Pod{}
	for _, p := range pods {
		if !utils.IsVirtualPod(p) {
			continue
		}
		podCopy := p.DeepCopy()
		utils.RecoverLabels(podCopy.Labels, podCopy.Annotations)
		podRefs = append(podRefs, podCopy)
	}

	return podRefs, nil
}

/*
// GetContainerLogs retrieves the logs of a container by name from the provider.
func (v *VirtualK8S) GetContainerLogs(ctx context.Context, namespace string,
	podName string, containerName string, opts ContainerLogOpts) (io.ReadCloser, error) {
		tailLine := int64(opts.Tail)
		limitBytes := int64(opts.LimitBytes)
		sinceSeconds := opts.SinceSeconds
		options := &corev1.PodLogOptions{
			Container:  containerName,
			Timestamps: opts.Timestamps,
			Follow:     opts.Follow,
		}
		if tailLine != 0 {
			options.TailLines = &tailLine
		}
		if limitBytes != 0 {
			options.LimitBytes = &limitBytes
		}
		if !opts.SinceTime.IsZero() {
			*options.SinceTime = metav1.Time{Time: opts.SinceTime}
		}
		if sinceSeconds != 0 {
			*options.SinceSeconds = int64(sinceSeconds)
		}
		if opts.Previous {
			options.Previous = opts.Previous
		}
		if opts.Follow {
			options.Follow = opts.Follow
		}
		logs := v.client.CoreV1().Pods(namespace).GetLogs(podName, options)
		stream, err := logs.Stream(ctx)
		if err != nil {
			return nil, fmt.Errorf("could not get stream from logs request: %v", err)
		}
return nil, nil
}
*/
/*
   // RunInContainer executes a command in a container in the pod, copying data
   // between in/out/err and the container's stdin/stdout/stderr.
   func (v *VirtualK8S) RunInContainer(ctx context.Context, namespace string, podName string, containerName string, cmd []string, attach api.AttachIO) error {
   		defer func() {
   			if attach.Stdout() != nil {
   				attach.Stdout().Close()
   			}
   			if attach.Stderr() != nil {
   				attach.Stderr().Close()
   			}
   		}()
   		req := v.client.CoreV1().RESTClient().
   			Post().
   			Namespace(namespace).
   			Resource("pods").
   			Name(podName).
   			SubResource("exec").
   			Timeout(0).
   			VersionedParams(&corev1.PodExecOptions{
   				Container: containerName,
   				Command:   cmd,
   				Stdin:     attach.Stdin() != nil,
   				Stdout:    attach.Stdout() != nil,
   				Stderr:    attach.Stderr() != nil,
   				TTY:       attach.TTY(),
   			}, scheme.ParameterCodec)

   		exec, err := remotecommand.NewSPDYExecutor(v.config, "POST", req.URL())
   		if err != nil {
   			return fmt.Errorf("could not make remote command: %v", err)
   		}

   		ts := &termSize{attach: attach}

   		err = exec.Stream(remotecommand.StreamOptions{
   			Stdin:             attach.Stdin(),
   			Stdout:            attach.Stdout(),
   			Stderr:            attach.Stderr(),
   			Tty:               attach.TTY(),
   			TerminalSizeQueue: ts,
   		})
   		if err != nil {
   			return err
   		}

return nil
}
*/

// NotifyPods instructs the notifier to call the passed in function when
// the pod status changes. It should be called when a pod's status changes.
//
// The provided pointer to a Pod is guaranteed to be used in a read-only
// fashion. The provided pod's PodStatus should be up to date when
// this function is called.
//
// NotifyPods will not block callers.
func (v *VirtualK8S) NotifyPods(ctx context.Context, f func(*corev1.Pod)) {
	klog.Info("Called NotifyPods")
	go func() {
		// to make sure pods have been add to known pods
		time.Sleep(10 * time.Second)
		for {
			select {
			case pod := <-v.updatedPod:
				klog.V(4).Infof("Enqueue updated pod %v", pod.Name)
				// need trim pod, e.g. UID
				utils.RecoverLabels(pod.Labels, pod.Annotations)
				f(pod)
			case <-v.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// createSecrets takes a Kubernetes Pod and deploys it within the provider.
func (v *VirtualK8S) createSecrets(ctx context.Context, secrets []string, ns string) error {
	for _, secretName := range secrets {
		_, err := v.clientCache.secretLister.Secrets(ns).Get(secretName)
		if err == nil {
			continue
		}
		if !errors.IsNotFound(err) {
			return err
		}
		secret, err := v.rm.GetSecret(secretName, ns)

		if err != nil {
			return err
		}
		utils.TrimObjectMeta(&secret.ObjectMeta)
		// skip service account secret
		if secret.Type == corev1.SecretTypeServiceAccountToken {
			if err := v.createServiceAccount(ctx, secret); err != nil {
				klog.Error(err)
				return err
			}
		}
		controllers.SetObjectGlobal(&secret.ObjectMeta)
		_, err = v.client.CoreV1().Secrets(ns).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			if errors.IsAlreadyExists(err) {
				continue
			}
			klog.Errorf("Failed to create secret %v err: %v", secretName, err)
			return fmt.Errorf("could not create secret %s in external cluster: %v", secretName, err)
		}
	}
	return nil
}

func (v *VirtualK8S) createServiceAccount(ctx context.Context, secret *corev1.Secret) error {
	if !v.enableServiceAccount {
		return nil
	}
	if secret.Annotations == nil {
		return fmt.Errorf("parse secret service account error")
	}
	klog.Infof("secret service-account info: [%v]", secret.Annotations)
	accountName, _ := secret.Annotations[corev1.ServiceAccountNameKey]
	if accountName == "" {
		err := fmt.Errorf("get secret of serviceAccount not exits: [%s] [%v]",
			secret.Name, secret.Annotations)
		return err
	}

	ns := secret.Namespace
	sa, err := v.client.CoreV1().ServiceAccounts(ns).Get(ctx, accountName, metav1.GetOptions{})
	if err != nil || sa == nil {
		klog.Infof("get serviceAccount [%v] err: [%v]]", sa, err)
		sa, err = v.client.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name: accountName,
			},
		}, metav1.CreateOptions{})
		klog.Errorf("create serviceAccount [%v] err: [%v]", sa, err)
		if err != nil {
			if errors.IsAlreadyExists(err) {
				return nil
			}
			return err
		}
	} else {
		klog.Infof("get secret serviceAccount info: [%s] [%v] [%v] [%v]",
			sa.Name, sa.CreationTimestamp, sa.Annotations, sa.UID)
	}
	secret.UID = sa.UID
	secret.Annotations[corev1.ServiceAccountNameKey] = accountName
	secret.Annotations[corev1.ServiceAccountUIDKey] = string(sa.UID)
	_, err = v.client.CoreV1().Secrets(ns).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		klog.Errorf("Failed to create secret %v err: %v", secret.Name, err)
	}

	sa.Secrets = []corev1.ObjectReference{{Name: secret.Name}}
	_, err = v.client.CoreV1().ServiceAccounts(ns).Update(ctx, sa, metav1.UpdateOptions{})
	if err != nil {
		klog.Infof(
			"update serviceAccount [%v] err: [%v]]",
			sa, err)
		return err
	}
	return nil
}

// createConfigMaps a Kubernetes Pod and deploys it within the provider.
func (v *VirtualK8S) createConfigMaps(ctx context.Context, configmaps []string, ns string) error {
	for _, cm := range configmaps {
		_, err := v.clientCache.cmLister.ConfigMaps(ns).Get(cm)
		if err == nil {
			continue
		}
		if errors.IsNotFound(err) {
			configMap, err := v.rm.GetConfigMap(cm, ns)
			if err != nil {
				return fmt.Errorf("find comfigmap %v error %v", cm, err)
			}
			utils.TrimObjectMeta(&configMap.ObjectMeta)
			controllers.SetObjectGlobal(&configMap.ObjectMeta)

			_, err = v.client.CoreV1().ConfigMaps(ns).Create(ctx, configMap, metav1.CreateOptions{})
			if err != nil {
				if errors.IsAlreadyExists(err) {
					continue
				}
				klog.Errorf("Failed to create configmap %v err: %v", cm, err)
				return err
			}
			klog.Infof("Create %v in %v success", cm, ns)
			continue
		}
		return fmt.Errorf("could not check configmap %s in external cluster: %v", cm, err)
	}
	return nil
}

// deleteConfigMaps a Kubernetes Pod and deploys it within the provider.
func (v *VirtualK8S) deleteConfigMaps(ctx context.Context, configmaps []string, ns string) error {
	for _, cm := range configmaps {
		err := v.client.CoreV1().ConfigMaps(ns).Delete(ctx, cm, metav1.DeleteOptions{})
		if err == nil {
			continue
		}
		if errors.IsNotFound(err) {
			continue
		}
		klog.Errorf("could not check configmap %s in external cluster: %v", cm, err)
		return err
	}
	return nil
}

// createPVCs a Kubernetes Pod and deploys it within the provider.
func (v *VirtualK8S) createPVCs(ctx context.Context, pvcs []string, ns string) error {
	for _, cm := range pvcs {
		_, err := v.client.CoreV1().PersistentVolumeClaims(ns).Get(ctx, cm, metav1.GetOptions{})
		if err == nil {
			continue
		}
		if errors.IsNotFound(err) {
			pvc, err := v.master.CoreV1().PersistentVolumeClaims(ns).Get(ctx, cm, metav1.GetOptions{})
			if err != nil {
				continue
			}
			utils.TrimObjectMeta(&pvc.ObjectMeta)
			controllers.SetObjectGlobal(&pvc.ObjectMeta)
			_, err = v.client.CoreV1().PersistentVolumeClaims(ns).Create(ctx, pvc, metav1.CreateOptions{})
			if err != nil {
				if errors.IsAlreadyExists(err) {
					continue
				}
				klog.Errorf("Failed to create pvc %v err: %v", cm, err)
				return err
			}
			continue
		}
		return fmt.Errorf("could not check pvc %s in external cluster: %v", cm, err)
	}
	return nil
}

/*// termSize helps exec termSize
type termSize struct {
	attach api.AttachIO
}

// Next returns the new terminal size after the terminal has been resized. It returns nil when
// monitoring has been stopped.
func (t *termSize) Next() *remotecommand.TerminalSize {
	resize := <-t.attach.Resize()
	return &remotecommand.TerminalSize{
		Height: resize.Height,
		Width:  resize.Width,
	}
}*/

func (v *VirtualK8S) createSA(ctx context.Context, sa string, ns string) (*corev1.ServiceAccount, error) {
	clientSA, err := v.client.CoreV1().ServiceAccounts(ns).Get(ctx, sa, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return nil, fmt.Errorf("could not check sa %s in member cluster: %v", sa, err)
	}

	// got
	if err == nil {
		return clientSA, nil
	}

	newSA := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sa,
			Namespace: ns,
		},
	}
	newSA, err = v.client.CoreV1().ServiceAccounts(ns).Create(ctx, newSA, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("could not create sa %s in member cluster: %v", sa, err)
	}

	return newSA, nil
}

func (v *VirtualK8S) createSAToken(ctx context.Context, saName string, ns string) (*corev1.Secret, error) {
	sa, err := v.master.CoreV1().ServiceAccounts(ns).Get(ctx, saName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not find sa %s in master cluster: %v", saName, err)
	}

	var secretName string
	if len(sa.Secrets) > 0 {
		secretName = sa.Secrets[0].Name
	}

	csName := fmt.Sprintf("master-%s-token", sa.Name)
	clientSecret, err := v.client.CoreV1().Secrets(ns).Get(ctx, csName, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return nil, fmt.Errorf("could not check secret %s in member cluster: %v", secretName, err)
	}
	if err == nil {
		return clientSecret, nil
	}

	masterSecret, err := v.master.CoreV1().Secrets(ns).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not find secret %s in master cluster: %v", secretName, err)
	}

	nData := map[string][]byte{}
	nData["token"] = masterSecret.Data["token"]

	se := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      csName,
			Namespace: ns,
		},
		Data: nData,
	}
	newSE, err := v.client.CoreV1().Secrets(ns).Create(ctx, se, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("could not create sa %s in member cluster: %v", sa, err)
	}

	return newSE, nil
}

func (v *VirtualK8S) createCA(ctx context.Context, ns string) (*corev1.ConfigMap, error) {

	masterCA, err := v.client.CoreV1().ConfigMaps(ns).Get(ctx, MasterRooTCAName, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return nil, fmt.Errorf("could not check configmap %s in member cluster: %v", MasterRooTCAName, err)
	}
	if err == nil {
		return masterCA, nil
	}

	ca, err := v.master.CoreV1().ConfigMaps(ns).Get(ctx, RooTCAConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not find configmap %s in master cluster: %v", ca, err)
	}

	newCA := ca.DeepCopy()
	newCA.Name = MasterRooTCAName
	utils.TrimObjectMeta(&newCA.ObjectMeta)

	newCA, err = v.client.CoreV1().ConfigMaps(ns).Create(ctx, newCA, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("could not create configmap %s in member cluster: %v", newCA.Name, err)
	}

	return newCA, nil
}

func (v *VirtualK8S) convertAuth(ctx context.Context, pod *corev1.Pod) {
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		falseValue := false
		pod.Spec.AutomountServiceAccountToken = &falseValue

		sa := pod.Spec.ServiceAccountName
		_, err := v.createSA(ctx, sa, pod.Namespace)
		if err != nil {
			klog.Errorf("[convertAuth] create sa failed, ns: %s, pod: %s", pod.Namespace, pod.Name)
			return
		}

		se, err := v.createSAToken(ctx, sa, pod.Namespace)
		if err != nil {
			klog.Errorf("[convertAuth] create sa secret failed, ns: %s, pod: %s", pod.Namespace, pod.Name)
			return
		}

		rootCA, err := v.createCA(ctx, pod.Namespace)
		if err != nil {
			klog.Errorf("[convertAuth] create sa secret failed, ns: %s, pod: %s", pod.Namespace, pod.Name)
			return
		}

		volumes := pod.Spec.Volumes
		for _, v := range volumes {
			if strings.HasPrefix(v.Name, SATokenPrefix) {
				sources := []corev1.VolumeProjection{}
				for _, src := range v.Projected.Sources {
					if src.ServiceAccountToken != nil {
						continue
					}
					if src.ConfigMap != nil && src.ConfigMap.Name == RooTCAConfigMapName {
						src.ConfigMap.Name = rootCA.Name
					}
					sources = append(sources, src)
				}

				secretProjection := corev1.VolumeProjection{
					Secret: &corev1.SecretProjection{
						Items: []corev1.KeyToPath{
							{
								Key:  "token",
								Path: "token",
							},
						},
					},
				}
				secretProjection.Secret.Name = se.Name
				sources = append(sources, secretProjection)
				v.Projected.Sources = sources
			}
		}

	}
}
