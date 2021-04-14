package backend

import (
	"fmt"
	"testing"

	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"
	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func TestDelete(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	key := fmt.Sprintf("/%s/%s/%s", randStringRunes(8), randStringRunes(8), randStringRunes(8))
	val := randStringRunes( /*1024 * 1024*/ 10)

	insertedRecord, err := be.Insert(key, []byte(val), 1)
	if err != nil {
		t.Fatalf("failed to insert with err :%v", err)
	}

	t.Logf("TestDelete inserted with rev:%v", insertedRecord.ModRevision())

	record, err := be.Delete(key, insertedRecord.ModRevision())
	if err != nil {
		t.Fatalf("failed to delete current record with err:%v", err)
	}

	if string(record.Value()) != string(val) {
		t.Fatalf("expected deleted record to carry matching values")
	}

	if record.CreateRevision() == record.ModRevision() {
		t.Fatalf("expected createRev != modRev got %v==%v", record.CreateRevision(), record.ModRevision())
	}

	if record.CreateRevision() != insertedRecord.ModRevision() {
		t.Fatalf("expected create rev to be %v instead got %v", record.CreateRevision(), insertedRecord.ModRevision())
	}

	// deleting the same record again should yeild into 404
	_, err = be.Delete(key, insertedRecord.ModRevision())
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
