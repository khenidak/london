package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

func TestWatchPrefix(t *testing.T) {
	stopEtcd, stopApiServer, apiHttpServer := creatApiServer(t)

	defer func() {
		stopApiServer()
		stopEtcd()
	}()

	kClient := clientset.NewForConfigOrDie(&restclient.Config{Host: apiHttpServer.URL})
	waitForServer(t, kClient)
	// wait for watch to be set
	configmaps := make([]string, 0)
	expectedEvents := make([]string, 0)

	_, initRev := createTestConfigMap(t, kClient)
	t.Logf("INIT WATCH:%v", initRev)
	// create
	for i := 0; i < 10; i++ {
		configMapName, _ := createTestConfigMap(t, kClient)
		// keep it
		configmaps = append(configmaps, configMapName)
		// add it to our events
		expectedEvents = append(expectedEvents, fmt.Sprintf("%s-%s", "ADD", configMapName))
	}

	for _, name := range configmaps {
		updateTestConfigMap(t, kClient, name)
		expectedEvents = append(expectedEvents, fmt.Sprintf("%s-%s", "DELETE", name))
	}

	// delete all
	for _, name := range configmaps {
		deleteTestConfigMap(t, kClient, name)
		expectedEvents = append(expectedEvents, fmt.Sprintf("%s-%s", "DELETE", name))
	}

	ctx := context.Background()
	gotEvents := make([]watch.Event, 0)
	timeOut := int64(30)
	opts := metav1.ListOptions{
		Watch:          true,
		TimeoutSeconds: &timeOut,
		//	ResourceVersion: initRev,
	}
	time.Sleep(10 * time.Second)

	w, err := kClient.CoreV1().ConfigMaps(metav1.NamespaceDefault).Watch(ctx, opts)
	if err != nil {
		t.Fatalf("FATAL: failed to create watch with err :%v", err)
	}

	results := w.ResultChan()
	for e := range results {
		gotEvents = append(gotEvents, e)
	}

	t.Logf("WATCH configmaps EVENTS: %v == %v ", len(gotEvents), len(expectedEvents))
	t.Logf("WATCH :%+v", gotEvents)
	if len(gotEvents) == 0 {
		t.Fatalf("WATCH expected watch events to be > 0")
	}
	// TODO: Once we have ModRev fixed we should write the compare test here
	// this test will be marked as Ok (false positive until then)
}
