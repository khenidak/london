package backend

import (
	"fmt"
	"testing"

	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func TestGet(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	key := fmt.Sprintf("/%s/%s/%s", randStringRunes(8), randStringRunes(8), randStringRunes(8))
	val := randStringRunes(1024 * 1024)

	rev, err := be.Insert(key, []byte(val), 1)
	if err != nil {
		t.Fatalf("failed to insert with err :%v", err)
	}

	t.Logf("TestGet inserted with rev:%v", rev)

	// get latest
	latestRecord, _, err := be.Get(key, 0)
	if err != nil {
		t.Fatalf("failed to get current record with err:%v", err)
	}

	if string(latestRecord.Value()) != val {
		t.Fatalf("Get latest record is not of the same value")
	}

	// get by rev
	record, _, err := be.Get(key, rev)
	if err != nil {
		t.Logf("failed to get by rev with err :%v", err)
	}

	if string(record.Value()) != val {
		t.Fatalf("Get record is not of the same value")
	}

}
