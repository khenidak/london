package e2e

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

type expectedEvent struct {
	objectName string
	eventType  string
}

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
	expectedEvents := make([]expectedEvent, 0)

	_, initRev := createTestConfigMap(t, kClient)
	t.Logf("INIT WATCH:%v", initRev)

	// create

	for i := 0; i < 3; i++ {
		configMapName, _ := createTestConfigMap(t, kClient)
		// keep it
		configmaps = append(configmaps, configMapName)
		// add it to our events
		expectedEvents = append(expectedEvents, expectedEvent{objectName: configMapName, eventType: "ADD"})
	}

	for _, name := range configmaps {
		updateTestConfigMap(t, kClient, name)
		expectedEvents = append(expectedEvents, expectedEvent{objectName: name, eventType: "UPDATE"})
	}

	// delete all
	for _, name := range configmaps {
		deleteTestConfigMap(t, kClient, name)
		expectedEvents = append(expectedEvents, expectedEvent{objectName: name, eventType: "DELETE"})
	}

	ctx := context.TODO()
	gotEvents := make([]watch.Event, 0)
	timeOut := int64(5) // should be more than enough to get all needed events

	opts := metav1.ListOptions{
		TimeoutSeconds:  &timeOut,
		ResourceVersion: initRev,
	}

	w, err := kClient.CoreV1().ConfigMaps(metav1.NamespaceDefault).Watch(ctx, opts)
	if err != nil {
		t.Fatalf("FATAL: failed to create watch with err :%v", err)
	}

	results := w.ResultChan()
	for e := range results {
		gotEvents = append(gotEvents, e)
	}

	if len(gotEvents) != len(expectedEvents) {
		t.Fatalf("WATCH expected watch events to be %v", len(expectedEvents))
	}

	for idx, e := range expectedEvents {
		actual := gotEvents[idx]

		actualObject, ok := actual.Object.(*v1.ConfigMap)
		if !ok {
			t.Fatalf("expected runtime object to be configmap")
		}

		if actualObject.Name != e.objectName {
			t.Fatalf("expected object name at %v to be %v got %v", idx, e.objectName, actualObject.Name)
		}

		if e.eventType == "ADD" {
			if actual.Type != watch.Added {
				t.Fatalf("expected event at %v to be add. got %v", idx, e.eventType)
			}
			continue
		}

		if e.eventType == "UPDATE" {
			if actual.Type != watch.Modified {
				t.Fatalf("expected event at %v to be updated. got %v", idx, e.eventType)
			}
			continue
		}

		if e.eventType == "DELETE" {
			if actual.Type != watch.Deleted {
				t.Fatalf("expected event at %v to be deleted. got %v", idx, e.eventType)
			}
			continue
		}

	}
}
