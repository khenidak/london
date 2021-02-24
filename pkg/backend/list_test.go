package backend

import (
	"fmt"
	"testing"

	"github.com/khenidak/london/pkg/backend/consts"
	basictestutils "github.com/khenidak/london/test/utils/basic"
)

type change struct {
	changeType string // ADD UPDATE DELETE
	key        string
	value      []byte
	modRev     int64
	prevRev    int64
}

func TestListForWatch(t *testing.T) {
	c := basictestutils.MakeTestConfig(t)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	// insert a 200 record
	data := make(map[string][]byte)
	revs := make(map[string]int64)
	baseKey := fmt.Sprintf("%s/%s/", randStringRunes(8), randStringRunes(8))
	keyFormat := "%s%s"
	changes := make([]change, 0)
	startRev, err := be.CurrentRevision()
	if err != nil {
		t.Fatalf("unexpected error reading current rev :%v", err)
	}
	t.Logf("start rev is:%v", startRev)
	for i := 0; i <= 20; i++ {
		key := fmt.Sprintf(keyFormat, baseKey, randStringRunes(8))
		// we insert small but we update big
		keyValue := []byte(randStringRunes(1024 * 1024))[:consts.DataRowMaxSize-1]

		data[key] = keyValue
		rev, err := be.Insert(key, keyValue, 1)
		if err != nil {
			t.Fatalf("failed to insert key with err :%v", err)
		}
		revs[key] = rev
		changes = append(changes, change{
			changeType: "ADD",
			key:        key,
			modRev:     rev,
			value:      keyValue,
		})
	}

	idx := 0

	// delete first 10 record
	for key, _ := range data {
		if idx == 9 {
			break
		}
		idx++
		deletedRecord, err := be.Delete(key, revs[key])
		if err != nil {
			t.Fatalf("failed to delete key with err;%v", err)
		}
		changes = append(changes, change{
			changeType: "DELETE",
			modRev:     deletedRecord.ModRevision(),
			prevRev:    deletedRecord.PrevRevision(),
			key:        key,
			value:      deletedRecord.Value(),
		})
		delete(data, key)
		delete(revs, key)
	}

	// update first 5
	idx = 0
	for key, _ := range data {
		if idx == 4 {
			break
		}
		idx++
		updatedValue := []byte(randStringRunes(1024 * 1024 * 10))[:consts.DataRowMaxSize*3]
		data[key] = updatedValue
		record, err := be.Update(key, updatedValue, revs[key], 0)
		if err != nil {
			t.Fatalf("failed to update key with err:%v", err)
		}
		changes = append(changes, change{
			changeType: "UPDATE",
			modRev:     record.ModRevision(),
			prevRev:    record.PrevRevision(),
			key:        key,
			value:      record.Value(),
		})
	}

	// insert more
	for i := 0; i <= 10; i++ {
		key := fmt.Sprintf(keyFormat, baseKey, randStringRunes(8))
		// we insert small but we update big
		keyValue := []byte(randStringRunes(1024 * 1024))[:consts.DataRowMaxSize-1]

		data[key] = keyValue
		rev, err := be.Insert(key, keyValue, 1)
		if err != nil {
			t.Fatalf("failed to insert key with err :%v", err)
		}
		revs[key] = rev
		changes = append(changes, change{
			changeType: "ADD",
			key:        key,
			modRev:     rev,
			value:      keyValue,
		})
	}

	records, err := be.ListForWatch(baseKey, startRev)
	if err != nil {
		t.Fatalf("failed to list for watch with err:%v", err)
	}
	t.Logf("Len records:%v len changes:%v", len(records), len(changes))

	if len(records) != len(changes) {
		t.Fatalf("unexpected len of records %v expected %v", len(records), len(changes))
	}

	for idx, record := range records {
		//recordKey := string(record.Key())
		if !record.IsEventRecord() {
			t.Fatalf("expected all records to be events. record at idx %v is not an event", idx)
		}

		if record.IsCreateEvent() && changes[idx].changeType != "ADD" {
			t.Fatalf("expected change ADD at idx %v got Create:%v Update:%v Delete:%v", idx, record.IsCreateEvent(), record.IsUpdateEvent(), record.IsDeleteEvent())
		}

		if record.IsDeleteEvent() && changes[idx].changeType != "DELETE" {
			t.Fatalf("expected change DELETE at idx %v got Create:%v Update:%v Delete:%v", idx, record.IsCreateEvent(), record.IsUpdateEvent(), record.IsDeleteEvent())
		}
		if record.IsUpdateEvent() && changes[idx].changeType != "UPDATE" {
			t.Fatalf("expected change UPDATE at idx %v got Create:%v Update:%v Delete:%v", idx, record.IsCreateEvent(), record.IsUpdateEvent(), record.IsDeleteEvent())
		}

		if record.ModRevision() != changes[idx].modRev {
			t.Fatalf("expected mod rev %v got %v at idx:%v", changes[idx].modRev, record.ModRevision(), idx)

		}

		if record.PrevRevision() != changes[idx].prevRev {
			t.Fatalf("expected prev rev %v got %v at idx:%v", changes[idx].modRev, record.PrevRevision(), idx)
		}
		if string(record.Value()) != string(changes[idx].value) {
			t.Fatalf("value do not match at idx:%v", idx)
		}

	}
}

func TestListForPrefix(t *testing.T) {
	c := basictestutils.MakeTestConfig(t)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	// insert a 200 record
	data := make(map[string][]byte)
	revs := make(map[string]int64)
	baseKey := fmt.Sprintf("%s/%s/", randStringRunes(8), randStringRunes(8))
	keyFormat := "%s/%s"

	for i := 0; i <= 20; i++ {
		key := fmt.Sprintf(keyFormat, baseKey, randStringRunes(8))
		// we insert small but we update big
		keyValue := []byte(randStringRunes(1024 * 1024))[:consts.DataRowMaxSize-1]

		data[key] = keyValue
		rev, err := be.Insert(key, keyValue, 1)
		if err != nil {
			t.Fatalf("failed to insert key with err :%v", err)
		}
		revs[key] = rev
	}

	// delete first 10 record
	idx := 0
	for key, _ := range data {
		if idx == 9 {
			break
		}
		idx++
		_, err := be.Delete(key, revs[key])
		if err != nil {
			t.Fatalf("failed to delete key with err;%v", err)
		}
		delete(data, key)
		delete(revs, key)
	}

	// update first 5
	idx = 0
	for key, _ := range data {
		if idx == 4 {
			break
		}
		idx++
		updatedValue := []byte(randStringRunes(1024 * 1024 * 10))[:consts.DataRowMaxSize*3]
		data[key] = updatedValue
		record, err := be.Update(key, updatedValue, revs[key], 0)
		if err != nil {
			t.Fatalf("failed to update key with err:%v", err)
		}
		revs[key] = record.ModRevision()
	}

	_, records, err := be.ListForPrefix(baseKey)
	if err != nil {
		t.Fatalf("failed to list current with err:%v", err)
	}

	for _, record := range records {
		key := string(record.Key())
		if _, ok := data[key]; !ok {
			t.Fatalf("found unexpected record %v", key)
		}

		if string(record.Value()) != string(data[key]) {
			t.Fatalf("returned record %v does not match data ", key)
		}

		if record.ModRevision() != revs[key] {
			t.Fatalf("returned record %v does not match expected revision", key)
		}

		delete(data, key)
		delete(revs, key)
	}

	if len(data) > 0 {
		t.Fatalf("some records were not returned in list %v", len(data))
	}
}
func TestListAllCurrent(t *testing.T) {
	c := basictestutils.MakeTestConfig(t)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	// insert a 200 record
	data := make(map[string][]byte)
	revs := make(map[string]int64)
	keyFormat := "/%s/%s/%s"
	for i := 0; i <= 20; i++ {
		key := fmt.Sprintf(keyFormat, randStringRunes(8), randStringRunes(8), randStringRunes(8))
		// we insert small but we update big
		keyValue := []byte(randStringRunes(1024 * 1024))[:consts.DataRowMaxSize-1]

		data[key] = keyValue
		rev, err := be.Insert(key, keyValue, 1)
		if err != nil {
			t.Fatalf("failed to insert key with err :%v", err)
		}
		revs[key] = rev
	}

	// delete first 10 record
	idx := 0
	for key, _ := range data {
		if idx == 9 {
			break
		}
		idx++
		_, err := be.Delete(key, revs[key])
		if err != nil {
			t.Fatalf("failed to delete key with err;%v", err)
		}
		delete(data, key)
		delete(revs, key)
	}

	// update first 5
	idx = 0
	for key, _ := range data {
		if idx == 4 {
			break
		}
		idx++
		updatedValue := []byte(randStringRunes(1024 * 1024 * 10))[:consts.DataRowMaxSize*3]
		data[key] = updatedValue
		record, err := be.Update(key, updatedValue, revs[key], 0)
		if err != nil {
			t.Fatalf("failed to update key with err:%v", err)
		}
		revs[key] = record.ModRevision()
	}

	_, records, err := be.ListAllCurrent()
	if err != nil {
		t.Fatalf("failed to list current with err:%v", err)
	}

	for _, record := range records {
		key := string(record.Key())
		if _, ok := data[key]; !ok {
			t.Fatalf("found unexpected record %v", key)
		}

		if string(record.Value()) != string(data[key]) {
			t.Fatalf("returned record %v does not match data ", key)
		}

		if record.ModRevision() != revs[key] {
			t.Fatalf("returned record %v does not match expected revision", key)
		}

		delete(data, key)
		delete(revs, key)
	}

	if len(data) > 0 {
		t.Fatalf("some records were not returned in list %v", len(data))
	}
}
