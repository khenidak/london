package e2e

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"k8s.io/kubernetes/test/integration/framework"

	testutils "github.com/khenidak/london/test/utils"
	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func creatApiServer(t *testing.T) (stopEtcdApi func(), stopApiServer func(), apiHttpServer *httptest.Server) {
	t.Helper()
	c := basictestutils.MakeTestConfig(t, false)
	// disable mtls
	c.UseTlS = false

	// run etcd api server
	stopEtcdApi = testutils.CreateTestApp(c, t)

	// create new opts
	opts := framework.DefaultEtcdOptions()
	// override listen address
	opts.StorageConfig.Transport.ServerList = []string{"http://localhost:2379"}
	masterOpts := &framework.MasterConfigOptions{opts}
	cfg := framework.NewIntegrationTestMasterConfigWithOptions(masterOpts)

	// run an api server with modified etcd address
	_, apiHttpServer, stopApiServer = framework.RunAMaster(cfg)

	return stopEtcdApi, stopApiServer, apiHttpServer

}

func waitForServer(t *testing.T, client *clientset.Clientset) {
	// Wait until the default "kubernetes" service is created.
	if err := wait.Poll(250*time.Millisecond, 5*time.Second, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(context.TODO(), "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return !apierrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}
}
func waitForInformerSync(t *testing.T, informer cache.SharedIndexInformer) {
	// Wait until the default "kubernetes" service is created.
	if err := wait.Poll(250*time.Millisecond, 5*time.Second, func() (bool, error) {
		return informer.HasSynced(), nil
	}); err != nil {
		t.Fatalf("FAIL: wait for informer sync has failed")
	}
}

func createTestNamespace(t *testing.T, client *clientset.Clientset) string {
	t.Helper()
	ns := &v1.Namespace{}

	ns.Name = testutils.RandObjectName(8)

	_, err := client.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create namespace err:%v", err)
	}
	return ns.Name
}

func deleteTestNamespace(t *testing.T, client *clientset.Clientset, name string) {
	t.Helper()
	err := client.CoreV1().Namespaces().Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete namespace err:%v", err)
	}
}

func updateTestNamespace(t *testing.T, client *clientset.Clientset, name string) {
	t.Helper()
	n, err := client.CoreV1().Namespaces().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get namespace %v err:%v", name, err)
	}
	n.Labels = map[string]string{"key": "value"}

	_, err = client.CoreV1().Namespaces().Update(context.TODO(), n, metav1.UpdateOptions{})

	if err != nil {
		t.Fatalf("failed to update namespace err:%v", err)
	}
}

func createTestConfigMap(t *testing.T, client *clientset.Clientset) (string, string) {
	t.Helper()
	configMap := &v1.ConfigMap{}

	configMap.Name = testutils.RandObjectName(8)

	configMap, err := client.CoreV1().ConfigMaps(metav1.NamespaceDefault).Create(context.TODO(), configMap, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create configmap err:%v", err)
	}
	return configMap.Name, configMap.ResourceVersion
}

func deleteTestConfigMap(t *testing.T, client *clientset.Clientset, name string) {
	t.Helper()
	err := client.CoreV1().ConfigMaps(metav1.NamespaceDefault).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete err:%v", err)
	}
}

func updateTestConfigMap(t *testing.T, client *clientset.Clientset, name string) {
	t.Helper()
	n, err := client.CoreV1().ConfigMaps(metav1.NamespaceDefault).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get configmap %v err:%v", name, err)
	}
	n.Labels = map[string]string{"key": testutils.RandObjectName(8)}

	_, err = client.CoreV1().ConfigMaps(metav1.NamespaceDefault).Update(context.TODO(), n, metav1.UpdateOptions{})

	if err != nil {
		t.Fatalf("failed to update configmap err:%v", err)
	}
}
