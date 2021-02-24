package backend

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/storage"
	"testing"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"
	"github.com/khenidak/london/pkg/backend/storerecord"

	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func TestDelete(t *testing.T) {
	c := basictestutils.MakeTestConfig(t)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	key := fmt.Sprintf("/%s/%s/%s", randStringRunes(8), randStringRunes(8), randStringRunes(8))
	val := randStringRunes( /*1024 * 1024*/ 10)

	rev, err := be.Insert(key, []byte(val), 1)
	if err != nil {
		t.Fatalf("failed to insert with err :%v", err)
	}

	t.Logf("TestDelete inserted with rev:%v", rev)

	record, err := be.Delete(key, rev)
	if err != nil {
		t.Fatalf("failed to delete current record with err:%v", err)
	}

	if string(record.Value()) != string(val) {
		t.Fatalf("expected deleted record to carry matching values")
	}

	if record.CreateRevision() == record.ModRevision() {
		t.Fatalf("expected createRev != modRev got %v==%v", record.CreateRevision(), record.ModRevision())
	}

	if record.CreateRevision() != rev {
		t.Fatalf("expected create rev to be %v instead got %v", record.CreateRevision(), rev)
	}

	// deleting the same record again should yeild into 404
	_, err = be.Delete(key, rev)
	if err == nil {
		t.Fatalf("repeated delete should yeild into an error")
	}

	if !storageerrors.IsNotFoundError(err) {
		t.Fatalf("repeated delete should yield into not found err :%v", err)
	}

	// deleting non existing record should yeild into 404
	_, err = be.Delete(key+"idontexist", 79)
	if !storageerrors.IsNotFoundError(err) {
		t.Fatalf("delete non exitent record should yield into not found err :%v", err)
	}
}

func TestCompactDelete(t *testing.T) {

	c := basictestutils.MakeTestConfig(t)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	// insert, then delete  10 records
	lastRev := int64(0)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("/%s/%s/%s", randStringRunes(8), randStringRunes(8), randStringRunes(8))
		val := randStringRunes(1024 * 1024)

		rev, err := be.Insert(key, []byte(val), 1)
		if err != nil {
			t.Fatalf("failed to insert with err :%v", err)
		}

		_, err = be.Delete(key, rev)
		if err != nil {
			t.Fatalf("failed to delete with err:%v", err)
		}
		lastRev = rev
	}

	t.Logf("compacting less than:%v", lastRev)
	// let us compact anything below that rev
	_, err = be.DeleteAllBeforeRev(lastRev)
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
