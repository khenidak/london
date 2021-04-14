package integration

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.etcd.io/etcd/clientv3"

	testutils "github.com/khenidak/london/test/utils"
	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func TestIntegrationWatch(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)
	stopfn := testutils.CreateTestApp(c, t)
	defer stopfn()

	client := testutils.MakeTestEtcdClient(c, t)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*5)
	defer cancel()

	firstComponent := testutils.RandStringRunes(8)
	secondComponent := testutils.RandStringRunes(8)

	keyFormat := "/%s/%s/%s"
	prefix := fmt.Sprintf("/%s/%s/", firstComponent, secondComponent)
	t.Logf("for this test 1st component:%v 2nd component:%v", firstComponent, secondComponent)
	testData := map[string]string{
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "alpha"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "bravo"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "charlie"):  testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "delta"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "echo"):     testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "foxtrot"):  testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "golf"):     testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "hotel"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "india"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "juliett"):  testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "kilo"):     testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "lima"):     testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "mike"):     testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "november"): testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "oscar"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "papa"):     testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "quebec"):   testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "romeo"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "sierra"):   testutils.RandStringRunes(16),
	}

	newKeys := []string{"tango", "uniform", "victor", "whiskey", "x-ray", "yankee", "zulu"}

	insertRevs := map[string]int64{}
	changesMade := map[string]*string{} // nil entry means deleted
	orderdChangeKeys := make([]string, 0, len(testData))

	// start inserting testing data
	lastRev := int64(0)
	for key, value := range testData {
		resp, err := client.Put(ctx, key, value)
		if err != nil {
			t.Fatalf("failed to insert test data %v:%v with err:%v", key, value, err)
		}

		lastRev = resp.Header.Revision
		// keep copy of rev of this key
		insertRevs[key] = lastRev
	}

	for k, _ := range testData {
		change := rand.Intn(4-1) + 1

		switch change {
		case 1: // update
			t.Logf("updating key:%v", k)
			newVal := testutils.RandStringRunes(16)
			updateRes, err := client.KV.Txn(ctx).If(
				clientv3.Compare(clientv3.ModRevision(k), "=", insertRevs[k]),
			).Then(
				clientv3.OpPut(k, newVal),
			).Else(
				clientv3.OpGet(k),
			).Commit()
			if err != nil {
				t.Fatalf("failed to update entry with error:%v", err)
			}
			if !updateRes.Succeeded {
				t.Fatalf("failed to update key result was not success")
			}

			changesMade[k] = &newVal
			orderdChangeKeys = append(orderdChangeKeys, k)
			/*
				case 2: // delete
					t.Logf("deleting key(%v):%v", insertRevs[k], k)

					createRes, err := client.KV.Txn(ctx).If(
						clientv3.Compare(clientv3.ModRevision(k), "=", insertRevs[k]),
					).Then(
						clientv3.OpDelete(k),
					).Else(
						clientv3.OpGet(k),
					).Commit()

					if err != nil {
						t.Fatalf("failed to delete entry with error:%v", err)
					}
					if !createRes.Succeeded {
						t.Fatalf("failed to create key result was not success")
					}

					changesMade[k] = nil
					orderdChangeKeys = append(orderdChangeKeys, k)
			*/
		case 3: // none
			t.Logf("not touching key:%v", k)
		}
	}

	// add new keys
	for idx, key := range newKeys {
		actualKey := fmt.Sprintf(keyFormat, firstComponent, secondComponent, key)
		testData[actualKey] = testutils.RandStringRunes(16)
		val := testData[actualKey]
		changesMade[actualKey] = &val
		orderdChangeKeys = append(orderdChangeKeys, actualKey)

		newKeys[idx] = actualKey
		t.Logf("creating new key %v:%v", actualKey, testData[actualKey])
		_, err := client.KV.Put(ctx, actualKey, testData[actualKey])
		if err != nil {
			t.Fatalf("failed to insert key with err:%v", err)
		}
	}

	t.Logf("===== done creating changes ======= ")

	// generate changes
	events := make([]*clientv3.Event, 0, len(testData))
	t.Logf("watch is starting at:%v", lastRev)
	ctx = clientv3.WithRequireLeader(ctx)

	// run multiple watchers to simulate multiple clients
	_ = client.Watch(ctx, prefix, clientv3.WithRev(lastRev))
	_ = client.Watch(ctx, prefix, clientv3.WithRev(lastRev))
	_ = client.Watch(ctx, prefix, clientv3.WithRev(lastRev))

	watchChan := client.Watch(ctx, prefix, clientv3.WithRev(lastRev))

	for we := range watchChan {
		t.Logf("recv-ed %v new events", len(we.Events))
		events = append(events, we.Events...)
	}

	// You will need this if things go wrong here
	/*
		for _, e := range events {
			t.Logf("REV %v KEY:%v TYPE:%v Mod:%v Create:%v", e.Kv.ModRevision, string(e.Kv.Key), e.Type, e.IsModify(), e.IsCreate())
		}
	*/
	// compare events
	if len(events) != len(orderdChangeKeys) {
		t.Fatalf("expected len of events %v got %v", len(orderdChangeKeys), len(events))
	}

	// we need to compare each change to ordered change and latest value
	for idx, e := range events {
		changedKey := string(e.Kv.Key)
		if orderdChangeKeys[idx] != changedKey {
			t.Fatalf("expected change key at idx:%v to be %v got %v", idx, orderdChangeKeys[idx], changedKey)
		}
		// because it exists in orderedList then it must exist in changesMade
		// we will not check that
		if e.Type == clientv3.EventTypeDelete {
			if changesMade[changedKey] != nil {
				t.Fatalf("expected change to be PUT but got change DELETE for key:%v at idx:%v", changedKey, idx)
			}

			if testData[changedKey] != string(e.PrevKv.Value) {
				t.Fatalf("expected DELETE event to carry old value %v for key %v at idx:%v", changedKey, testData[changedKey], idx)
			}

			continue
		}

		if e.IsModify() {
			if changesMade[changedKey] == nil {
				t.Fatalf("expected change to be DELETE but got change PUT for key:%v at idx:%v", changedKey, idx)
			}

			if *(changesMade[changedKey]) != string(e.Kv.Value) {
				t.Fatalf("expected new keyvalue %v got %v", changesMade[changedKey], string(e.Kv.Value))
			}
			continue
		}

		if e.IsCreate() {
			// ensure that key is in our newKeys
			found := false
			for _, newKey := range newKeys {
				if newKey == changedKey {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("found ADD but not it is not in our newkeys for key:%v", changedKey)
			}

			if changesMade[changedKey] == nil {
				t.Fatalf("expected change to be ADD :%v at idx:%v", changedKey, idx)
			}
			if *(changesMade[changedKey]) != string(e.Kv.Value) {
				t.Fatalf("expected new keyvalue changedKey:%v val: %v got %v", changedKey, *changesMade[changedKey], string(e.Kv.Value))
			}
		}
	}

}
