package backend

import (
	"time"

	klogv2 "k8s.io/klog/v2"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/backend/utils"
	"github.com/khenidak/london/pkg/types"
)

// Delete performs deletes (on current record by
// 1. delete current record
// 2. create new record mark it as delete (record carries value, old revision)
func (s *store) Delete(key string, revision int64) (types.Record, error) {
	klogv2.Infof("STORE-DELETE:%v-%v", key, revision)
	validKey := storerecord.CreateValidKey(key)
	validRevision := storerecord.RevToString(revision)

	// get new rev
	newRev, err := s.rev.Increment()
	if err != nil {
		return nil, err
	}

	f := filterutils.NewFilter()
	f.And(
		filterutils.Partition(validKey),
		filterutils.CurrentWithRevision(validRevision),
		filterutils.ExcludeEvents(),
	)
	f.Or(
		filterutils.CombineAnd(
			filterutils.IncludeDataRows(validRevision),
			filterutils.ExcludeEvents(),
		),
	)

	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}

	res, err := utils.SafeExecuteQuery(s.t, consts.DefaultTimeout, storage.FullMetadata, o)
	if err != nil {
		return nil, err
	}
	// no results
	if len(res.Entities) == 0 {
		return nil, storage.AzureStorageServiceError{StatusCode: 404}
	}

	currentRecord, err := storerecord.NewFromEntities(res.Entities, false)
	if err != nil {
		// we could do better error reporting here, since faults in
		// data records will look like 404
		return nil, storage.AzureStorageServiceError{StatusCode: 404}
	}

	newRecord, err := storerecord.NewForDeleted(newRev, currentRecord)
	if err != nil {
		return nil, err
	}

	event := storerecord.CreateEventEntityFromRecord(newRecord)

	batch := s.t.NewBatch()
	// we can not change partition key, so we have to delete current record
	currentEntity := currentRecord.RowEntity()
	batch.DeleteEntity(currentEntity, false)

	// now we create a current record
	newRowEntity := newRecord.RowEntity()
	newRowEntity.Table = s.t
	batch.InsertEntity(newRowEntity)
	// insert data entities
	for _, dataEntity := range newRecord.DataEntities() {
		dataEntity.Table = s.t
		batch.InsertEntity(dataEntity)
	}

	// insert event
	event.Table = s.t
	batch.InsertEntity(event)

	err = utils.SafeExecuteBatch(batch)
	if err != nil {
		return nil, err
	}

	return newRecord, nil
}

func (s *store) DeleteAllBeforeRev(rev int64) (int64, error) {
	// this compacts the storage by removing all (non current keys) with rev less than rev
	// in order to minimize the total calls this func produce we batch based on partition keys
	// keeping each batch at max of 99 (azure storage has a 100 max entries per batch)

	const batchSize = 99
	start := time.Now()

	totalKeys := 0
	totalRev := 0
	totalReq := 0
	toBatch := make(map[string][]string)
	var lastErr error
	defer func() {
		if lastErr != nil {
			klogv2.Infof("failed to complete compact request in:%v totalRequests:%d ms", time.Since(start), totalReq)
			return
		}
		klogv2.Infof("completed compact requet keys:%v revs:%v using %v requests in %v", totalKeys, totalRev, totalReq, time.Since(start))
	}()

	// adds a rev grouped by key
	addToBatch := func(key string, rev string) {
		if _, ok := toBatch[key]; !ok {
			toBatch[key] = make([]string, 0, batchSize)
			totalKeys = totalKeys + 1
		}

		totalRev = totalRev + 1
		toBatch[key] = append(toBatch[key], rev)
	}

	// finds if a key is ready to be batched
	shouldExecuteBatch := func(key string, ignoreCount bool) error {
		if !ignoreCount && len(toBatch[key]) < batchSize {
			return nil
		}

		batch := s.t.NewBatch()
		for _, rev := range toBatch[key] {
			entity := &storage.Entity{}
			entity.Table = s.t
			entity.PartitionKey = key
			entity.RowKey = rev
			batch.DeleteEntity(entity, true)
		}
		// now we have a max of 99 records in one
		// batch (and should be less than 4MB azure's
		// max. because we add no values)
		totalReq = totalReq + 1
		err := utils.SafeExecuteBatch(batch)
		if err != nil {
			return err
		}
		return nil
	}

	// get records
	var res *storage.EntityQueryResult
	f := filterutils.NewFilter()
	f.And(
		filterutils.RevisionLessThan(storerecord.RevToString(rev)),
		filterutils.ExcludeCurrent(),
		filterutils.ExcludeSysRecords(),
	)

	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}
	res, lastErr = utils.SafeExecuteQuery(s.t, consts.DefaultTimeout, storage.NoMetadata, o)
	if lastErr != nil {
		return 0, lastErr
	}

	for {
		if len(res.Entities) == 0 {
			break
		}

		for _, e := range res.Entities {
			// add it
			addToBatch(e.PartitionKey, e.RowKey)
			lastErr = shouldExecuteBatch(e.PartitionKey, false)
			if lastErr != nil {
				return 0, lastErr
			}
		}

		if res.NextLink == nil {
			break
		}
		res, lastErr = res.NextResults(nil) // TODO: <-- is this correct??
		if lastErr != nil {
			return 0, lastErr
		}

	}

	// add this point we may have keys with revs that are less than
	// batch size. We have to delete them and yes they will produce batches
	// with less than batch optimum size. Because we call execBatch with everyadd
	// the reminder for each key *is* less than batchsize and can go in one call
	for key, _ := range toBatch {
		lastErr = shouldExecuteBatch(key, true)
		if lastErr != nil {
			return 0, lastErr
		}
	}

	// we will attempt to get current rev.
	// but should be safe to ignore errors here

	currentRev, _ := s.rev.Current()
	return currentRev, nil
}
