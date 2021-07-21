package backend

import (
	"github.com/Azure/azure-sdk-for-go/storage"

	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/backend/utils"
	"github.com/khenidak/london/pkg/types"
)

// Delete performs deletes (on current record by
// 1. delete current record
// 2. create new record mark it as delete (record carries value, old revision)
func (s *store) Delete(key string, revision int64) (types.Record, error) {
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

	entities, err := s.execQueryFull(storage.FullMetadata, o)
	if err != nil {
		return nil, err
	}
	// no results
	if len(entities) == 0 {
		return nil, storage.AzureStorageServiceError{StatusCode: 404}
	}

	currentRecord, err := storerecord.NewFromEntities(entities, false)
	if err != nil {
		// we could do better error reporting here, since faults in
		// data records will look like 404
		return nil, storage.AzureStorageServiceError{StatusCode: 404}
	}

	newRecord, err := storerecord.NewForDeleted(newRev, currentRecord)
	if err != nil {
		return nil, storage.AzureStorageServiceError{StatusCode: 404}
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
