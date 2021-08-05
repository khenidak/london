package backend

import (
	"fmt"
	"testing"

	"github.com/khenidak/london/pkg/backend/consts"
	basictestutils "github.com/khenidak/london/test/utils/basic"
)

/*
// we no longer need to test for ListByPrefix.
// TODO REMOVE TEST AND FUNC
func TestListForPrefix(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

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
		insertedRecord, err := be.Insert(key, keyValue, 1)
		if err != nil {
			t.Fatalf("failed to insert key with err :%v", err)
		}
		rev := insertedRecord.ModRevision()
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
		_, record, err := be.Update(key, updatedValue, revs[key], 0)
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
*/
func TestListAllCurrent(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, true)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	// insert a 10 record
	data := make(map[string][]byte)
	revs := make(map[string]int64)
	keyFormat := "/%s/%s/%s"
	for i := 0; i <= 10; i++ {
		key := fmt.Sprintf(keyFormat, randStringRunes(8), randStringRunes(8), randStringRunes(8))
		// we insert small but we update big
		keyValue := []byte(randStringRunes(1024 * 1024))[:consts.DataRowMaxSize-1]

		data[key] = keyValue
		insertedRecord, err := be.Insert(key, keyValue, 1)
		if err != nil {
			t.Fatalf("failed to insert key with err :%v", err)
		}
		rev := insertedRecord.ModRevision()

		revs[key] = rev
	}

	// delete first 5 record
	idx := 0
	for key, _ := range data {
		if idx == 4 {
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
		_, record, err := be.Update(key, updatedValue, revs[key], 0)
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

type testEvent struct {
	key       string
	rev       int64
	eventType int
}

func TestListEvents(t *testing.T) {

	c := basictestutils.MakeTestConfig(t, true)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	// insert a 10 record
	startRev := int64(1000000)
	data := make(map[string][]byte)
	revs := make(map[string]int64)
	events := make([]testEvent, 0)

	const eventTypeAdd = 0
	const eventTypeDelete = 1
	const eventTypeUpdate = 2

	keyFormat := "/%s/%s/%s"
	for i := 0; i <= 10; i++ {
		key := fmt.Sprintf(keyFormat, randStringRunes(8), randStringRunes(8), randStringRunes(8))
		// we insert small but we update big
		keyValue := []byte(randStringRunes(10))

		data[key] = keyValue
		insertedRecord, err := be.Insert(key, keyValue, 1)
		if err != nil {
			t.Fatalf("failed to insert key with err :%v", err)
		}
		rev := insertedRecord.ModRevision()
		if rev < startRev {
			startRev = rev
		}
		revs[key] = rev
		events = append(events, testEvent{
			key:       key,
			rev:       rev,
			eventType: eventTypeAdd,
		})
	}

	// delete first 3 record
	idx := 0
	for key, _ := range data {
		if idx == 2 {
			break
		}
		idx++
		record, err := be.Delete(key, revs[key])
		if err != nil {
			t.Fatalf("failed to delete key with err;%v", err)
		}
		delete(data, key)
		events = append(events, testEvent{
			key:       key,
			rev:       record.ModRevision(),
			eventType: eventTypeDelete,
		})

	}

	// update first 2
	idx = 0
	for key, _ := range data {
		if idx == 2 {
			break
		}
		idx++
		updatedValue := []byte(randStringRunes(1024 * 1024 * 10))[:consts.DataRowMaxSize*3]
		data[key] = updatedValue
		_, record, err := be.Update(key, updatedValue, revs[key], 0)
		if err != nil {
			t.Fatalf("failed to update key with err:%v", err)
		}
		events = append(events, testEvent{
			key:       key,
			rev:       record.ModRevision(),
			eventType: eventTypeUpdate,
		})
	}

	t.Logf("start rev is:%v", startRev)
	gotEvents, err := be.ListEvents(startRev)
	if err != nil {
		t.Fatalf("failed to list events with err:%v", err)
	}

	if len(gotEvents) != len(events) {
		t.Fatalf("expected len of events:%v got %v", len(events), len(gotEvents))
	}

	for idx, gotEvent := range gotEvents {
		savedEvent := events[idx]
		if string(gotEvent.Key()) != savedEvent.key {
			t.Fatalf("event:%v expected key %v got %v", idx, savedEvent.key, gotEvent.Key())
		}

		if gotEvent.IsCreateEvent() && (savedEvent.eventType != eventTypeAdd) {
			t.Fatalf("event:%v is Create which was not expected", idx)
		}

		if gotEvent.IsDeleteEvent() && (savedEvent.eventType != eventTypeDelete) {
			t.Fatalf("event:%v is Delete which was not expected", idx)
		}

		// must be an update
		if !gotEvent.IsCreateEvent() && !gotEvent.IsDeleteEvent() && savedEvent.eventType != eventTypeUpdate {
			t.Fatalf("event:%v is Update which was not expected", idx)
		}
	}
}
