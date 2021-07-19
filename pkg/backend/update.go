package backend

import (
	//klogv2 "k8s.io/klog/v2"

	"github.com/Azure/azure-sdk-for-go/storage"

	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/backend/utils"
	"github.com/khenidak/london/pkg/types"
)

// Updates performs an update on existing (current) record by
// 1. updates current with rev, value, lease
// 2. create a new record that carries old value and revision (as row key)
func (s *store) Update(key string, val []byte, revision int64, lease int64) (types.Record, types.Record, error) {
	// klogv2.Infof("STORE-UPDATE:%v-%v", key, revision)
	validKey := storerecord.CreateValidKey(key)

	// get new rev
	newRev, err := s.rev.Increment()
	if err != nil {
		return nil, nil, err
	}
	// we operate only on the row entity here. Meaning:
	// for old record we get only row entity
	// for new record we insert row + data entities

	// pkey == key && rKey == current && revision == revision
	f := filterutils.NewFilter()
	f.And(
		filterutils.Partition(validKey),
		filterutils.CurrentKeysOnly(),
		filterutils.ExcludeEvents(),
	)

	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}

	entites, err := s.execQueryFull(storage.FullMetadata, o)
	if err != nil {
		return nil, nil, err
	}

	// no results
	if len(entites) == 0 {
		return nil, nil, storage.AzureStorageServiceError{StatusCode: 404}
	}

	currentRecord, err := storerecord.NewFromEntities(entites, true)
	if err != nil {
		return nil, nil, err
	}

	if currentRecord.ModRevision() != revision {
		// record is not at current revision
		getRecord, _, err := s.GetIfNeeded(currentRecord.RowEntity(), true)
		if err != nil {
			return nil, nil, err
		}
		//		klogv2.Infof("update conflict:key:%v\nwant:%v\nstored:%v", key, revision, currentRecord.ModRevision())
		return getRecord, nil, storage.AzureStorageServiceError{StatusCode: 409}
	}

	batch := s.t.NewBatch()

	updatedRecord, err := storerecord.NewForUpdate(newRev, val, currentRecord, lease)
	if err != nil {
		return nil, nil, err
	}

	event := storerecord.CreateEventEntityFromRecord(updatedRecord)

	mergeEntity := updatedRecord.RowEntity()
	mergeEntity.Table = s.t

	// merge updated row entity into
	batch.MergeEntity(mergeEntity)
	// add the data records for updated Entity
	for _, e := range updatedRecord.DataEntities() {
		e.Table = s.t
		batch.InsertEntity(e)
	}

	// insert the row Entity for current (which should have been modified)
	insertEntity := currentRecord.RowEntity()
	insertEntity.Table = s.t
	batch.InsertEntity(insertEntity)

	// insert event
	event.Table = s.t
	batch.InsertEntity(event)

	err = utils.SafeExecuteBatch(batch)
	if err != nil {
		return nil, nil, err
	}
	return currentRecord, updatedRecord, nil
}
