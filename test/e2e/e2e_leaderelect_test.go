package e2e

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"

	testutils "github.com/khenidak/london/test/utils"
)

func TestLeaderElect(t *testing.T) {
	stopEtcd, stopApiServer, apiHttpServer := creatApiServer(t)

	defer func() {
		stopApiServer()
		stopEtcd()
	}()

	kClient := clientset.NewForConfigOrDie(&restclient.Config{Host: apiHttpServer.URL})
	waitForServer(t, kClient)

	myId := testutils.RandObjectName(8)
	leaseObjectName := testutils.RandObjectName(8)
	leaseObjectNamespace := testutils.RandObjectName(8)

	t.Logf("testing leader-elect with \nid:%s\nname:%s\nnamespace:%s",
		myId, leaseObjectName, leaseObjectNamespace)

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseObjectName,
			Namespace: leaseObjectNamespace,
		},
		Client: kClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: myId,
		},
	}

	gotLease := false
	run := func(ctx context.Context) {
		// complete your controller loop here
		t.Logf("Controller loop...")
		gotLease = true
		time.Sleep(time.Second * 5)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	// start the leader election code loop
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock,
		// IMPORTANT: you MUST ensure that any code you have that
		// is protected by the lease must terminate **before**
		// you call cancel. Otherwise, you could have a background
		// loop still running and another process could
		// get elected before your background loop finished, violating
		// the stated goal of the lease.
		ReleaseOnCancel: true,
		LeaseDuration:   5 * time.Second,
		RenewDeadline:   3 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// we're notified when we start - this is where you would
				// usually put your code
				t.Logf("*** lease lock acquired")
				run(ctx)
			},
			OnStoppedLeading: func() {
				// we can do cleanup here
				t.Logf("*** lease lost: %s", myId)
				cancel()
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if identity == myId {
					t.Logf("*** lease was correctly elected")
					return
				}
				t.Fatalf("**** different leader was elected: %s", identity)
			},
		},
	})

	if !gotLease {
		t.Fatalf("failed to acquire lease")
	}
}
