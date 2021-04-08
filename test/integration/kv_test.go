package integration

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/clientv3"

	testutils "github.com/khenidak/london/test/utils"
	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func BenchmarkPut(b *testing.B) {
	client, stop := getClient(b, false)
	defer stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		integrationPut(b, client)
	}
}

func getClient(tb testing.TB, clearTable bool) (*clientv3.Client, func()) {
	c := basictestutils.MakeTestConfig(tb, clearTable)
	stopfn := testutils.CreateTestApp(c, tb)

	client := testutils.MakeTestEtcdClient(c, tb)
	return client, stopfn
}

func TestIntegrationPut(t *testing.T) {
	client, stop := getClient(t, false)
	defer stop()
	integrationPut(t, client)
}

// Put is not used by kubernetes, however we built it to ensure basic
// etcdclient works as expected
func integrationPut(t testing.TB, client *clientv3.Client) {

	k := testutils.RandKey(16)
	v := testutils.RandStringRunes(16)

	ctx := context.TODO()
	_, err := client.Put(ctx, k, v)

	if err != nil {
		t.Fatalf("failed to put with err:%v", err)
	}

	getResp, err := client.Get(ctx, k)
	if err != nil {
		t.Fatalf("failed to get after put with err:%v", err)
	}

	if getResp.Count != 1 || string(getResp.Kvs[0].Value) != v {
		t.Fatalf("value after put is not as expected")
	}
}

func TestIntegrationRangeGet(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)
	stopfn := testutils.CreateTestApp(c, t)
	defer stopfn()

	k := testutils.RandKey(16)
	v := testutils.RandStringRunes(16)

	client := testutils.MakeTestEtcdClient(c, t)
	ctx := context.TODO()

	_, err := client.Put(ctx, k, v)
	if err != nil {
		t.Fatalf("failed to pre insert with err :%v", err)
	}

	resp, err := client.Get(ctx, k)

	if err != nil {
		t.Fatalf("failed to get with err:%v", err)
	}

	if len(resp.Kvs) != 1 {
		t.Fatalf("expected response count to be 1 got %+v", resp)
	}

	if string(resp.Kvs[0].Value) != v {
		t.Fatalf("expected response value to = %v got %v", v, resp.Kvs[0].Value)
	}
}

func TestIntegrationTxnInsert(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)
	stopfn := testutils.CreateTestApp(c, t)
	defer stopfn()

	k := testutils.RandKey(16)
	v := testutils.RandStringRunes(16)

	client := testutils.MakeTestEtcdClient(c, t)
	ctx := context.TODO()

	_, err := client.Put(ctx, k, v)
	if err != nil {
		t.Fatalf("failed to insert with err :%v", err)
	}

	resp, err := client.Get(ctx, k)
	if err != nil {
		t.Fatalf("failed to get with err:%v", err)
	}

	if resp.Count != 1 || string(resp.Kvs[0].Value) != v {
		t.Fatalf("expected value after insert does not match")
	}
}

func TestIntegrationTxnDelete(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)
	stopfn := testutils.CreateTestApp(c, t)
	defer stopfn()

	k := testutils.RandKey(16)
	v := testutils.RandStringRunes(16)

	client := testutils.MakeTestEtcdClient(c, t)
	ctx := context.TODO()

	res, err := client.Put(ctx, k, v)
	if err != nil {
		t.Fatalf("failed to pre insert with err :%v", err)
	}

	rev := res.Header.Revision

	txnResp, err := client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(k), "=", rev),
	).Then(
		clientv3.OpDelete(k),
	).Else(
		clientv3.OpGet(k),
	).Commit()

	if err != nil {
		t.Fatalf("failed to delete entry with error:%v", err)
	}

	if !txnResp.Succeeded {
		t.Fatalf("delete tx failed:%+v", txnResp)
	}

	getResponse, err := client.Get(ctx, k)

	if err != nil {
		t.Fatalf("failed to get with err:%v", err)
	}

	if getResponse.Count != 0 {
		t.Fatalf("get after delete should return zero records:%+v", getResponse)
	}
}

func BenchmarkUpdate(b *testing.B) {
	client, stop := getClient(b, false)
	defer stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intgrationUpdate(client, b)
	}
}

func TestIntgrationUpdate(t *testing.T) {
	client, stop := getClient(t, false)
	defer stop()
	intgrationUpdate(client, t)
}

func intgrationUpdate(client *clientv3.Client, t testing.TB) {
	k := testutils.RandKey(16)
	v := testutils.RandStringRunes(16)
	updatedv := testutils.RandStringRunes(16)

	ctx := context.TODO()

	res, err := client.Put(ctx, k, v)
	if err != nil {
		t.Fatalf("failed to pre insert with err :%v", err)
	}

	rev := res.Header.Revision
	txnResp, err := client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(k), "=", rev),
	).Then(
		clientv3.OpPut(k, updatedv),
	).Else(
		clientv3.OpGet(k),
	).Commit()

	if err != nil {
		t.Fatalf("failed to update entry with error:%v", err)
	}

	if !txnResp.Succeeded {
		t.Fatalf("update tx failed:%+v", txnResp)
	}

	getResponse, err := client.Get(ctx, k)
	if err != nil {
		t.Fatalf("failed to get with err:%v", err)
	}

	if getResponse.Count == 0 {
		t.Fatalf("get after update should return *one* record:%+v", getResponse)
	}

	if string(getResponse.Kvs[0].Value) != updatedv {
		t.Fatalf("expected value after update %v got %v", updatedv, getResponse.Kvs[0].Value)
	}

	// try update with wrong rev
	updatedv = "fail"
	txnFailedUpdate, err := client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(k), "=", getResponse.Kvs[0].ModRevision-1),
	).Then(
		clientv3.OpPut(k, updatedv),
	).Else(
		clientv3.OpGet(k),
	).Commit()

	if err != nil {
		t.Fatalf("failed to perform a failed update err:%v", err)
	}

	if txnFailedUpdate.Succeeded {
		t.Fatalf("expected transaction to fail but it was successful")
	}

	// we should expect the record here using the same old value we just updated it with
	if len(txnFailedUpdate.Responses) != 1 {
		t.Fatalf("there should be one response in responses, len: %v", len(txnFailedUpdate.Responses))
	}
}

func TestIntgrationCompact(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)
	stopfn := testutils.CreateTestApp(c, t)
	defer stopfn()

	client := testutils.MakeTestEtcdClient(c, t)
	ctx := context.TODO()

	lastRev := int64(0)
	// after compation, these key:val
	// should be the current value
	keyLastVal := make(map[string]string)
	baseKeyName := testutils.RandStringRunes(16)
	// lastRev := 0
	// 20 key, a 50 update each
	t.Logf("about to insert compact test data. This will take a bit (10 keys 10 updates each)")
	for i := 1; i <= 10; i++ {
		k := fmt.Sprintf("%s--%v", baseKeyName, i)
		keyLastVal[k] = ""
		putRes, err := client.Put(ctx, k, "0")
		if err != nil {
			t.Fatalf("failed to insert compaction test data err:%v", err)
		}

		lastRev = putRes.Header.Revision

		for j := 0; j <= 20; j++ {
			lastVal := fmt.Sprintf("%v-%v", k, j)
			keyLastVal[k] = lastVal

			txnResp, err := client.KV.Txn(ctx).If(
				clientv3.Compare(clientv3.ModRevision(k), "=", lastRev),
			).Then(
				clientv3.OpPut(k, lastVal),
			).Else(
				clientv3.OpGet(k),
			).Commit()

			if err != nil {
				t.Fatalf("failed to update compact test data with err:%v", err)
			}

			lastRev = txnResp.Header.Revision
		}
	}

	// let us compact data.
	t.Logf("done setting up test data.")
	t.Logf("compacting Rev:%v", lastRev)
	compactResp, err := client.Compact(ctx, lastRev)
	if err != nil {
		t.Fatalf("failed to execute compact with err:%v", err)
	}

	t.Logf("after compact rev is at:%v", compactResp.Header.Revision)
	// test that last values match the expectations
	for key, val := range keyLastVal {
		resp, err := client.Get(ctx, key)
		if err != nil {
			t.Fatalf("failed to get err:%v", err)
		}

		if resp.Count != 1 {
			t.Fatalf("expected one record but got %v", resp.Count)
		}

		if string(resp.Kvs[0].Value) != val {
			t.Fatalf("expected value for key:%v to be %v got %v", key, val, string(resp.Kvs[0].Value))
		}
	}
}

func BenchmarkRangeList(b *testing.B) {
	client, stop := getClient(b, false)
	defer stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intgrationRaneList(client, b)
	}
}

func TestIntgrationRangeList(t *testing.T) {
	client, stop := getClient(t, false)
	defer stop()
	intgrationRaneList(client, t)
}

func intgrationRaneList(client *clientv3.Client, t testing.TB) {
	ctx := context.TODO()
	firstComponent := testutils.RandStringRunes(8)
	secondComponent := testutils.RandStringRunes(8)

	keyFormat := "/%s/%s/%s"
	prefix := fmt.Sprintf("/%s/%s/", firstComponent, secondComponent)
	//t.Logf("for this test 1st component:%v 2nd component:%v", firstComponent, secondComponent)
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
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "tango"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "uniform"):  testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "victor"):   testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "whiskey"):  testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "x-ray"):    testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "yankee"):   testutils.RandStringRunes(16),
		fmt.Sprintf(keyFormat, firstComponent, secondComponent, "zulu"):     testutils.RandStringRunes(16),
	}
	// start inserting testing data
	for key, value := range testData {
		_, err := client.Put(ctx, key, value)
		if err != nil {
			t.Fatalf("failed to insert test data %v:%v with err:%v", key, value, err)
		}
	}
	listResponse, err := client.KV.Get(context.Background(),
		prefix,
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)))
	if err != nil {
		t.Fatalf("failed to list for prefix")
	}

	if len(listResponse.Kvs) != len(testData) {
		t.Fatalf("expected len of response %v got %v", len(testData), len(listResponse.Kvs))
	}

	// verify each record in return
	for _, kv := range listResponse.Kvs {
		currentVal, ok := testData[string(kv.Key)]
		if !ok {
			t.Fatalf("failed to find %v in test data", string(kv.Key))
		}
		if currentVal != string(kv.Value) {
			t.Fatalf("expected key %v value to be:%v got %v", string(kv.Key), currentVal, string(kv.Value))
		}
	}

	// verify that all records exist
	for k, _ := range testData {
		found := false
		for _, kv := range listResponse.Kvs {
			if string(kv.Key) == k {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("could not find key:%v in returned result", k)
		}
	}
}
