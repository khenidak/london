package frontend

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/khenidak/london/pkg/backend"
	"github.com/khenidak/london/pkg/config"
	"github.com/khenidak/london/pkg/types"
	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func makeBackend(t *testing.T) (*config.Config, backend.Backend) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := backend.NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	return c, be
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

func randStringRunes(n int) string {
	rand.Seed(time.Now().UnixNano())

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestPrefixFromKey(t *testing.T) {
	testCases := []struct {
		name           string
		key            string
		expectedPrefix string
	}{
		{
			name:           "key1",
			key:            "/a/b/object",
			expectedPrefix: "/a/b/",
		},
		{
			name:           "noslashes",
			key:            "/object",
			expectedPrefix: "/",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			output := prefixFromKey(testCase.key)
			if output != testCase.expectedPrefix {
				t.Fatalf("expected prefix for key %v is %v got %v", testCase.key, testCase.expectedPrefix, output)
			}

		})
	}
}

func insertListOfLists(be backend.Backend, t *testing.T) map[string]map[string]string {
	t.Helper()
	// create 3 x 4 list
	l1c1 := randStringRunes(8)
	l1c2 := randStringRunes(8)

	l2c1 := randStringRunes(8)
	l2c2 := randStringRunes(8)

	l3c1 := randStringRunes(8)
	l3c2 := randStringRunes(8)

	lists := map[string]map[string]string{
		fmt.Sprintf("/%s/%s/", l1c1, l1c2): map[string]string{
			fmt.Sprintf("/%s/%s/%s", l1c1, l1c2, randStringRunes(8)): randStringRunes(8),
			fmt.Sprintf("/%s/%s/%s", l1c1, l1c2, randStringRunes(8)): randStringRunes(8),
			fmt.Sprintf("/%s/%s/%s", l1c1, l1c2, randStringRunes(8)): randStringRunes(8),
		},
		fmt.Sprintf("/%s/%s/", l2c1, l2c2): map[string]string{
			fmt.Sprintf("/%s/%s/%s", l2c1, l2c2, randStringRunes(8)): randStringRunes(8),
			fmt.Sprintf("/%s/%s/%s", l2c1, l2c2, randStringRunes(8)): randStringRunes(8),
			fmt.Sprintf("/%s/%s/%s", l2c1, l2c2, randStringRunes(8)): randStringRunes(8),
		},
		fmt.Sprintf("/%s/%s/", l3c1, l3c2): map[string]string{
			fmt.Sprintf("/%s/%s/%s", l3c1, l3c2, randStringRunes(8)): randStringRunes(8),
			fmt.Sprintf("/%s/%s/%s", l3c1, l3c2, randStringRunes(8)): randStringRunes(8),
			fmt.Sprintf("/%s/%s/%s", l3c1, l3c2, randStringRunes(8)): randStringRunes(8),
		},
	}

	// insert:
	for _, list := range lists {
		for k, v := range list {
			_, err := be.Insert(k, []byte(v), 0)
			if err != nil {
				t.Fatalf("failed to insert k/v with err:%v", err)
			}
		}
	}

	return lists
}

func compareLists(saved map[string]string, to []types.Record, t *testing.T) {
	t.Helper()

	if len(to) != len(saved) {
		t.Fatalf("expected len of records %v got %v", len(saved), len(to))
	}

	for k, v := range saved {
		found := false
		for _, r := range to {
			if k == string(r.Key()) {
				found = true
				if v != string(r.Value()) {
					t.Fatalf("value for key %v does not match", v)
				}
			}
		}

		if !found {
			t.Fatalf("failed to find key:%v in list", k)
		}
	}
}

/*
func TestColdLoad(t *testing.T) {
	c, be := makeBackend(t)
	lm := newListManagerForTest(c, be)

	savedLists := insertListOfLists(be, t)

	// run one iteration
	// TODO: @khenidak - loadevents should return an error
	// this test will remain flaky until we do that
	lm.mgmtLoadEventsIteration()

	// get and compare lists
	for prefix, savedList := range savedLists {
		gotList, err := lm.list(prefix)
		if err != nil {
			t.Fatalf("failed to list with err %v", err)
		}
		compareLists(savedList, gotList, t)
	}
}

func TestLoadViaEvents(t *testing.T) {
	c, be := makeBackend(t)
	lm := newListManagerForTest(c, be)

	savedLists := insertListOfLists(be, t)

	// run the loop once
	lm.mgmtLoadEventsIteration()

	// get and compare lists
	// we are reading the cached one, not forcing load
	for prefix, savedList := range savedLists {
		gotList, err := lm.getList(prefix, false)
		if err != nil {
			t.Fatalf("failed to list with err %v", err)
		}

		compareLists(savedList, gotList.toRecords(), t)
	}
}
*/
type fakeRecord struct {
	key       string
	value     string
	modRev    int64
	isEvent   bool
	isDeleted bool
	isUpdated bool
	isCreated bool
}

func (fr *fakeRecord) Key() []byte           { return []byte(fr.key) }
func (fr *fakeRecord) Value() []byte         { return []byte(fr.value) }
func (fr *fakeRecord) ModRevision() int64    { return fr.modRev }
func (fr *fakeRecord) CreateRevision() int64 { return fr.modRev }
func (fr *fakeRecord) PrevRevision() int64   { return fr.modRev }
func (fr *fakeRecord) Lease() int64          { return int64(0) }
func (fr *fakeRecord) IsEventRecord() bool   { return fr.isEvent }
func (fr *fakeRecord) IsDeleteEvent() bool   { return fr.isDeleted }
func (fr *fakeRecord) IsUpdateEvent() bool   { return fr.isUpdated }
func (fr *fakeRecord) IsCreateEvent() bool   { return fr.isCreated }

func TestLoadWithNotifications(t *testing.T) {
	c, be := makeBackend(t)
	lm := newListManagerForTest(c, be)

	savedLists := insertListOfLists(be, t)

	// run the loop once
	lm.mgmtLoadEventsIteration()

	// notify
	// collect prefixes
	keys := make([]string, 0, 3)
	for k, _ := range savedLists {
		keys = append(keys, k)
	}

	// add..edit..delete
	for idx, key := range keys {
		for k, _ := range savedLists[key] {
			// for simplicty we are operating on first key
			if idx == 0 { // add fake one
				addedKey := fmt.Sprintf("%s%s", key, randStringRunes(8))
				addedValue := randStringRunes(8)
				savedLists[key][addedKey] = addedValue
				lm.notifyInserted(&fakeRecord{
					key:       addedKey,
					value:     addedValue,
					modRev:    9223372036854775807,
					isCreated: true,
				})
				break
			}

			if idx == 1 { // modify
				modVal := randStringRunes(8)
				savedLists[key][k] = modVal
				lm.notifyUpdated(&fakeRecord{
					key:       k,
					value:     modVal,
					modRev:    9223372036854775806,
					isUpdated: true,
				},
					&fakeRecord{
						key:       k,
						value:     modVal,
						modRev:    9223372036854775807,
						isUpdated: true,
					})
				break
			}

			if idx == 2 { // delete
				lm.notifyDeleted(&fakeRecord{
					key:       k,
					value:     savedLists[key][k],
					modRev:    9223372036854775807,
					isDeleted: true,
				})

				delete(savedLists[key], k)
				break
			}
		}
	}

	// get and compare lists
	// we are reading the cached one, not forcing load
	for prefix, savedList := range savedLists {
		gotList, err := lm.getList(prefix, false)
		if err != nil {
			t.Fatalf("failed to list with err %v", err)
		}

		compareLists(savedList, gotList.toRecords(), t)
	}
}

func TestEventsCompact(t *testing.T) {
	c, be := makeBackend(t)
	lm := newListManagerForTest(c, be)

	testCases := []struct {
		name             string
		revs             [][]int64
		compactRev       int64
		expectedRevAfter [][]int64 // must be reversed since list appends new to head
	}{
		{
			name:             "empty events",
			revs:             [][]int64{},
			compactRev:       9,
			expectedRevAfter: [][]int64{},
		},
		{
			name: "keeping all events",
			revs: [][]int64{
				{10, 20, 30},
				{100, 200, 300},
				{1000, 2000, 3000},
			},
			compactRev: 0,
			expectedRevAfter: [][]int64{
				{3000, 2000, 1000},
				{300, 200, 100},
				{30, 20, 10},
			},
		},
		{
			name: "compact all events",
			revs: [][]int64{
				{10, 20, 30},
				{100, 200, 300},
				{1000, 2000, 3000},
			},
			compactRev:       5000,
			expectedRevAfter: [][]int64{},
		},
		{
			name: "lose last",
			revs: [][]int64{
				{10, 20, 30},
				{100, 200, 300},
				{1000, 2000, 3000},
			},
			compactRev: 50,
			expectedRevAfter: [][]int64{
				{3000, 2000, 1000},
				{300, 200, 100},
			},
		},
		{
			name: "lose all except first",
			revs: [][]int64{
				{10, 20, 30},
				{100, 200, 300},
				{1000, 2000, 3000},
			},
			compactRev: 500,
			expectedRevAfter: [][]int64{
				{3000, 2000, 1000},
			},
		},
		{
			name: "cut middle last",
			revs: [][]int64{
				{10, 20, 30},
				{100, 200, 300},
				{1000, 2000, 3000},
			},
			compactRev: 15,
			expectedRevAfter: [][]int64{
				{3000, 2000, 1000},
				{300, 200, 100},
				{30, 20},
			},
		},

		{
			name: "cut in the middle",
			revs: [][]int64{
				{10, 20, 30},
				{100, 200, 300},
				{1000, 2000, 3000},
			},
			compactRev: 150,
			expectedRevAfter: [][]int64{
				{3000, 2000, 1000},
				{300, 200},
			},
		},
		{
			name: "cut middle first",
			revs: [][]int64{
				{10, 20, 30},
				{100, 200, 300},
				{1000, 2000, 3000},
			},
			compactRev: 1500,
			expectedRevAfter: [][]int64{
				{3000, 2000},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			baseKey := fmt.Sprintf("%s/%s/", randStringRunes(8), randStringRunes(8))

			list, _ := lm.getList(baseKey, false)
			// add events
			for _, revList := range testCase.revs {
				addEvents := make([]types.Record, 0)
				for _, rev := range revList {
					e := &fakeRecord{
						key:       fmt.Sprintf("%s%s", baseKey, randStringRunes(8)),
						value:     randStringRunes(8),
						modRev:    rev,
						isCreated: true,
					}
					addEvents = append(addEvents, e)
				}
				list.processEvents(addEvents)
			}

			// call compact
			list.compactEvents(testCase.compactRev)

			// compare results
			if len(list.events) != len(testCase.expectedRevAfter) {
				t.Fatalf("expected events lists len:%v got %v", len(testCase.expectedRevAfter), len(list.events))
			}

			// for each list compare
			for idx, eventsSlice := range list.events {
				if len(eventsSlice) != len(testCase.expectedRevAfter[idx]) {
					t.Fatalf("at idx:%v expected len:%v got :%v", idx, len(testCase.expectedRevAfter[idx]), len(eventsSlice))
				}

				// compare each
				for innerIdx, event := range eventsSlice {
					if event.ModRevision() != testCase.expectedRevAfter[idx][innerIdx] {
						t.Fatalf("list %v at %v expected Rev:%v got %v", idx, innerIdx, testCase.expectedRevAfter[idx][innerIdx], event.ModRevision())
					}
				}

			}
		})
	}
}
