package backend

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/storage"
	"testing"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/backend/storerecord"

	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func TestCompactDelete(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	// insert, then delete  10 records
	lastRev := int64(0)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("/%s/%s/%s", randStringRunes(8), randStringRunes(8), randStringRunes(8))
		val := randStringRunes(1024 * 1024)

		insertedRecord, err := be.Insert(key, []byte(val), 1)
		if err != nil {
			t.Fatalf("failed to insert with err :%v", err)
		}

		_, err = be.Delete(key, insertedRecord.ModRevision())
		if err != nil {
			t.Fatalf("failed to delete with err:%v", err)
		}
		lastRev = insertedRecord.ModRevision()
	}

	t.Logf("compacting less than:%v", lastRev)
	// let us compact anything below that rev
	_, err = be.Compact(lastRev)
	if err != nil {
		t.Fatalf("failed to compact with err:%v", err)
	}
	// find records that are less than that rev
	f := filterutils.NewFilter()
	f.And(
		filterutils.RevisionLessThan(storerecord.RevToString(lastRev)),
		filterutils.ExcludeCurrent(),
		filterutils.ExcludeSysRecords(),
	)

	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}
	res, err := c.Runtime.StorageTable.QueryEntities(consts.DefaultTimeout, storage.NoMetadata, o)
	if err != nil {
		t.Fatalf("failed to query with err:%v", err)
	}

	if len(res.Entities) > 0 {
		t.Fatalf("expected to find zero entities with rev < %v found %v", lastRev, len(res.Entities))
	}
}
