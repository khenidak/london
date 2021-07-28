package frontend

import (
	"fmt"
	klogv2 "k8s.io/klog/v2"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/khenidak/london/pkg/backend"
	"github.com/khenidak/london/pkg/config"
	"github.com/khenidak/london/pkg/types"
)

// List manager is a cache of lists.
// each list is a list of keys that matches a prefix e.g. /x/y/z
// ListManager is a "current" view of lists. It does not include
// older versions.

// items in a list gets updated via two mechanisms:
// 1. loading events and updating internal state (mgmt loop)
// 2 direct notification from action that happened on the backend. That ensures
// that read-after-write will always return the updated version.

const maxFail = 4

type listManager struct {
	// high water mark for events we have acquired from
	// store
	revHighWatermark int64

	// represents the lowest (compacted) events we can
	// acquire from store
	revLowWatermark int64

	// if reaches max we will panic, to avoid responding stale data
	errorCount int

	config *config.Config
	be     backend.Backend
	lists  sync.Map // of *list
}

type list struct {
	loadLock sync.Mutex

	// events is an order list of lists. New first.
	// all watchers read from cached set of events
	// the events gets updated using two ways:
	// events loop
	// notification on write from front the front end.
	// * events are subject to compactation (from memory)
	eventsLock sync.RWMutex
	events     [][]types.Record

	firstRead bool
	prefix    string
	items     sync.Map // of listItem
}

type listItem struct {
	lock sync.Mutex
	r    types.Record
}

func newlistManager(config *config.Config, be backend.Backend, revLowWatermark int64) *listManager {
	lm := &listManager{
		config: config,
		be:     be,
		// last mod-rev loaded from store
		revHighWatermark: revLowWatermark,
		// last mod-rev marked as compacted
		revLowWatermark: revLowWatermark,
	}

	// run events load one iteration to
	// ensure that all lists are ready

	// start mgmtLoop
	go lm.mgmtLoop()
	return lm
}

// for test only
func newListManagerForTest(config *config.Config, be backend.Backend) *listManager {
	lm := &listManager{
		config: config,
		be:     be,
	}
	// tests run the iteration manually
	return lm
}

// mgmtLoop responsible for reading events from
// backend and use them to update internal state
func (lm *listManager) mgmtLoop() {
	ticker := time.NewTicker(time.Millisecond * 50)
	stopCh := lm.config.Runtime.Context.Done()

	lastCompact := time.Now().UTC()
	for {
		select {
		case <-stopCh:
			// done here
			return
		case <-ticker.C:
			lm.mgmtLoadEventsIteration()
			// getting the events is more important than
			// filtering out compacted events. Even at the
			// cost of some more more mem usage.

			window := lastCompact.Add(time.Minute * 1)
			nowTime := time.Now().UTC()
			if nowTime.After(window) {
				start := time.Now()

				klogv2.Errorf("list-manager: (event compact) did not run in the last 1m.. running")
				lastCompact = time.Now().UTC()
				lm.mgmtFilterCompactedEventsIteration()

				duration := time.Since(start)
				// in theory we need to keep that under 250ms
				klogv2.Errorf("list-manager: (event compact) ran for %v", duration)
			}
		}
	}
}

func (lm *listManager) mgmtFilterCompactedEventsIteration() {
	// set current compacted rev
	compactedRev, err := lm.be.GetCompactedRev(true)

	if err != nil {
		klogv2.Errorf("list-manager: (event compact) failed to read compacted rev from store with err:%v. will try again later", err)
		return
	}
	if compactedRev <= lm.revLowWatermark {
		// nothing to do here
		klogv2.Error("list-manager: (event compact) compacted rev less or equal to cached compacted rev, ignoring this run")
		return
	}

	// set
	lm.revLowWatermark = compactedRev

	//for each list..
	lm.lists.Range(
		func(k, v interface{}) bool {
			// we can run this a goroutine. but we want to avoid
			// WLock all lists (events) at the same time. making it
			// latent to read /get events from all of them
			// we relay on the fact that the entire process
			// runs against in-mem lists. hence each is a short
			// lived o(n) loop per list
			thisList := v.(*list)
			thisList.compactEvents(lm.revLowWatermark)
			return true
		})
}

// get all events since last run
// for each event get the list, update the list accordingly
func (lm *listManager) mgmtLoadEventsIteration() {
	events, err := lm.be.ListEvents(lm.revHighWatermark + 1)
	if err != nil {
		lm.errorCount++
		if lm.errorCount == maxFail {
			panic(fmt.Sprintf("list cache loop has failed %v/%v times, crash and reload (err:%v)", lm.errorCount, maxFail, err))
		}
		klogv2.Errorf("list-manager:failed to read events with err:%v -- still under max fail %v/%v", err, lm.errorCount, maxFail)
		return // error but under max fail
	}

	lm.errorCount = 0 // reset error count
	byPrefix := make(map[string][]types.Record)

	// pivot the list by prefix,
	// so we won't need to get/re-get lists
	for _, eventRecord := range events {
		// increase rev if needed
		lm.trySetLastRev(eventRecord.ModRevision())
		prefix := prefixFromKey(string(eventRecord.Key()))
		if _, ok := byPrefix[prefix]; !ok {
			byPrefix[prefix] = make([]types.Record, 0)
		}

		byPrefix[prefix] = append(byPrefix[prefix], eventRecord)
	}

	// we can now process events by prefix
	for prefix, eventsForPrefix := range byPrefix {
		// we can saftely ingore error here
		// since we are not going through a path that would generate an error
		l, _ := lm.getList(prefix, false)
		l.processEvents(eventsForPrefix)
	}
}

func (lm *listManager) trySetLastRev(newRev int64) {
	if newRev > lm.revHighWatermark {
		lm.revHighWatermark = newRev
	}
}

// returns the current view of a list.
func (lm *listManager) list(prefix string) ([]types.Record, error) {
	/* the problem with this function is, if there is no list already there
	be it loaded or not the result will always == 0
	this condition happens before the first run of events loop
	TODO: lm needs to declare READINESS. e.g. signal READY once the first
	event loop iteration happened.
	until then all list() calls will return empty results.

	The simplest most efficent solution is to run event loop iteration
	once in sync mode (as part of NewListManager() call)

	We can not load all since
	-- it may not exist as a prefix in our list
	-- we are not sure if the list is for
	   -- base prefix (i.e, all-namespaces) such as /registry/leases
		 -- specific namesoace such as /registry/pods/default/
	*/
	var records []types.Record
	lm.lists.Range(func(k, v interface{}) bool {
		key := k.(string)
		l := v.(*list)
		if strings.HasPrefix(string(key), prefix) {

			listRecords := l.toRecords()
			totalRecords := make([]types.Record, 0, len(records)+len(listRecords))
			totalRecords = append(totalRecords, listRecords...)
			totalRecords = append(totalRecords, records...)
			records = totalRecords
		}
		return true
	})

	sort.Slice(records[:], func(i, j int) bool {
		return string(records[i].Key()) > string(records[j].Key())
	})

	return records, nil
}

//events gets from cached events all events > rev
func (lm *listManager) events(prefix string, newerThanRev int64) ([]types.Record, error) {
	var records []types.Record
	lm.lists.Range(func(k, v interface{}) bool {
		key := k.(string)
		l := v.(*list)
		if strings.HasPrefix(string(key), prefix) {

			listRecords := l.getEvents(newerThanRev, lm.revLowWatermark)
			totalRecords := make([]types.Record, 0, len(records)+len(listRecords))
			totalRecords = append(totalRecords, listRecords...)
			totalRecords = append(totalRecords, records...)
			records = totalRecords
		}
		return true
	})

	// reverse the order of results..
	// we add new to old, but we need to return old to new
	sort.Slice(records, func(i, j int) bool {
		return records[i].ModRevision() < records[j].ModRevision()
	})
	return records, nil
}

func (lm *listManager) getList(prefix string, loadIfNotLoaded bool) (*list, error) {
	ifNotThere := &list{
		prefix: prefix,
		events: make([][]types.Record, 0),
	}
	actualI, _ := lm.lists.LoadOrStore(prefix, ifNotThere)
	actual := actualI.(*list)

	if loadIfNotLoaded {
		err := actual.load(lm.be)
		if err != nil {
			return nil, err
		}
	}

	return actual, nil
}

// remove any event older than rev
func (l *list) compactEvents(olderThan int64) {
	l.eventsLock.Lock()
	defer l.eventsLock.Unlock()

	if len(l.events) == 0 {
		return // it is empty
	}

	outerIdx := 0
	innerIdx := 0
	keepGoing := true
	for _, eventsSlice := range l.events {
		innerIdx = 0
		for _, event := range eventsSlice {
			if event.ModRevision() < olderThan {
				keepGoing = false
				break
			}
			innerIdx++
		}

		if !keepGoing {
			break
		}

		outerIdx++
	}

	if outerIdx == 0 {
		// base case: the entire list has been compacted
		if innerIdx == 0 {
			l.events = l.events[:0] // saves on re-alloc
			return
		}

		// we have only one inner list remaining, and it is partially gone
		l.events = l.events[:1]
		l.events[0] = l.events[0][:innerIdx]
		return
	}

	// last list is gone
	if innerIdx == 0 {
		l.events = l.events[:outerIdx]
		return
	}

	// somewhere in the middle.
	// trim outer
	if outerIdx == len(l.events) {
		// last list
		l.events = l.events[:outerIdx]
		l.events[outerIdx-1] = l.events[outerIdx-1][:innerIdx]
	} else {
		// somehwhere in the middle
		l.events[outerIdx] = l.events[outerIdx][:innerIdx]
		l.events = l.events[:outerIdx+1]
	}
}

// update events adds a slice of events to existing slice
// it does not perform checks on how old events are.
// instead it adds them to head of events we already cached.
func (l *list) addEvents(eventRecords []types.Record) {

	// cachevents, EXPECTED NEW TO BE ON HEAD
	l.eventsLock.Lock()
	defer l.eventsLock.Unlock()
	newSlice := make([][]types.Record, 0, len(l.events)+1)
	newSlice = append(newSlice, eventRecords)
	newSlice = append(newSlice, l.events...)
	l.events = newSlice
}

// updates list state based on events
// caches events for later use by watchers
func (l *list) processEvents(eventRecords []types.Record) {
	if len(eventRecords) == 0 {
		return
	}

	// ensure that they are ordered new-->old
	// so watchers don't need to wade through a long list.
	sort.Slice(eventRecords, func(i, j int) bool {
		return eventRecords[i].ModRevision() > eventRecords[j].ModRevision()
	})
	// events needs to be applied on list backward. old then new.
	for i := len(eventRecords) - 1; i >= 0; i-- {
		eventRecord := eventRecords[i]
		if eventRecord.IsCreateEvent() {
			l.inserted(eventRecord)
			continue
		}

		if eventRecord.IsDeleteEvent() {
			l.deleted(eventRecord)
			continue
		}

		l.updated(eventRecord)
	}
	// update cached events with these events
	// for watchers - if any - to pick it up
	l.addEvents(eventRecords)
}

func (l *list) getEvents(newerThanRev, revLowWatermark int64) []types.Record {
	all := make([]types.Record, 0, 128)

	l.eventsLock.RLock()
	defer l.eventsLock.RUnlock()

	for _, eventsSlice := range l.events {
		keepGoing := true

		for _, event := range eventsSlice {
			// filter out events that has been compacted or less than requested rev
			if event.ModRevision() <= newerThanRev || event.ModRevision() <= revLowWatermark {
				keepGoing = false
				break
			}
			all = append(all, event)
		}
		if !keepGoing {
			break
		}
	}

	return all
}

// converts a list to records, this op is o(n) but we it is a lot cheaper
// and alot faster than going to store
func (l *list) toRecords() []types.Record {
	all := make([]types.Record, 0, 128) // just avoid re-alloc + copy for the 128 items
	counter := 0
	l.items.Range(func(k, v interface{}) bool {
		asListItem := v.(*listItem)

		func() {
			asListItem.lock.Lock()
			defer asListItem.lock.Unlock()
			all = append(all, asListItem.r)
		}()

		counter++
		return true // keep going
	})

	return all[:counter]
}

func (l *list) load(be backend.Backend) error {
	if !l.firstRead {
		l.loadLock.Lock()
		defer l.loadLock.Unlock()
		if l.firstRead {
			// it was already between the double check
			return nil
		}

		// lock is held, ready to go
		// This is the only time we call list
		// to populate our internal state. the rest
		// is done via events and notification
		klogv2.V(0).Infof("Expensive: cold list for prefix:%v", l.prefix)
		_, records, err := be.ListForPrefix(l.prefix)
		if err != nil {
			return err
		}

		l.feedRecords(records)
		l.firstRead = true
	}

	return nil
}

// saftely adds records to list
func (l *list) feedRecords(records []types.Record) {
	for _, r := range records {
		// create new item
		item := &listItem{}
		item.r = r
		currentI, loaded := l.items.LoadOrStore(string(r.Key()), item)
		if !loaded {
			// done, here nothing more to do
			continue
		}

		// current is there. compare. new wins
		current := currentI.(*listItem)

		current.lock.Lock()
		defer current.lock.Unlock()

		if r.ModRevision() > current.r.ModRevision() {
			klogv2.V(8).Infof("REPLACE %v:%v=>%v ", string(current.r.Key()), current.r.ModRevision(), r.ModRevision())
			current.r = r
		}
	}
}

// gets a record from list
func (l *list) getRecord(key string) types.Record {
	// create new item
	currentI, loaded := l.items.Load(key)
	if !loaded {
		return nil
	}

	current := currentI.(*listItem)
	current.lock.Lock()
	defer current.lock.Unlock()
	return current.r
}

func (lm *listManager) getRecord(key string) types.Record {
	prefix := prefixFromKey(key)
	// we ignore error here because we don't reload
	l, _ := lm.getList(prefix, false)
	return l.getRecord(key)
}

func (l *list) inserted(r types.Record) {
	l.feedRecords([]types.Record{r})
}

func (l *list) updated(r types.Record) {
	l.feedRecords([]types.Record{r})
}

func (l *list) deleted(r types.Record) {
	l.items.Delete(string(r.Key()))
}

func (l *list) deletedByKey(key string) {
	l.items.Delete(key)
}

func (lm *listManager) notifyCompact(compactRev int64) {
	// hmm assuming that we are on 64b, no torn reads
	if lm.revLowWatermark < compactRev {
		lm.revLowWatermark = compactRev
	}
}

// used by front end to notify listManager of a record
// inserted
func (lm *listManager) notifyInserted(r types.Record) {
	prefix := prefixFromKey(string(r.Key()))
	// we ignore error here because we don't reload
	l, _ := lm.getList(prefix, false)
	l.inserted(r)
}

// used by front end to notify listManager of a deleted record
func (lm *listManager) notifyDeleted(r types.Record) {
	prefix := prefixFromKey(string(r.Key()))
	// we ignore error here because we don't reload
	l, _ := lm.getList(prefix, false)
	l.deleted(r)
}

// used by front end to notify listManager of a deleted record by "key"
func (lm *listManager) notifyDeletedKey(key string) {
	prefix := prefixFromKey(key)
	// we ignore error here because we don't reload
	l, _ := lm.getList(prefix, false)
	l.deletedByKey(key)
}

// used by front end to notify listManager of an updated record
func (lm *listManager) notifyUpdated(oldRecord, r types.Record) {
	prefix := prefixFromKey(string(r.Key()))
	// we ignore error here because we don't reload
	l, _ := lm.getList(prefix, false)

	// update list
	l.updated(r)
}

// from a key that looks like /x/y/z/<object>
// returns /x/y/z/
func prefixFromKey(key string) string {
	prefix := suffixedKey(filepath.Dir(key))
	if prefix == "./" {
		prefix = "/"
	}

	return prefix
}
