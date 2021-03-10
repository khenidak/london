package backend

import (
	"context"
	"fmt"
	"sort"

	"github.com/Azure/azure-sdk-for-go/storage"
	"go.opentelemetry.io/otel"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/types"
)

type entitiesBookKeepingFn func(entities []*storage.Entity)
type entitiesToRecordsFn func() ([]types.Record, error)

func (s *store) getDefaultBookingKeepingFuncs(forceSort bool) (entitiesBookKeepingFn, entitiesToRecordsFn) {
	orderedRowEntities := make([]*storage.Entity, 0)
	dataEntitiesByKeyRev := make(map[string][]*storage.Entity)
	keyFormat := "%s/%s"

	bookKeeping := func(entities []*storage.Entity) {
		for _, e := range entities {
			// row entities are kept as a stand alone
			if storerecord.IsRowEntity(e) || storerecord.IsEventEntity(e) {
				orderedRowEntities = append(orderedRowEntities, e)
				continue
			}
			// must be data entity
			// add them to their own list
			revision := e.Properties[consts.RevisionFieldName].(string)
			dataKey := fmt.Sprintf(keyFormat, e.PartitionKey, revision)
			if _, ok := dataEntitiesByKeyRev[dataKey]; !ok {
				dataEntitiesByKeyRev[dataKey] = make([]*storage.Entity, 0, 1)
			}
			dataEntitiesByKeyRev[dataKey] = append(dataEntitiesByKeyRev[dataKey], e)
		}
	}

	toRecords := func() ([]types.Record, error) {
		records := make([]types.Record, 0, len(orderedRowEntities))
		for _, thisRowEntity := range orderedRowEntities {
			revision := thisRowEntity.Properties[consts.RevisionFieldName].(string)
			dataKey := fmt.Sprintf(keyFormat, thisRowEntity.PartitionKey, revision)
			DataEntities := dataEntitiesByKeyRev[dataKey]

			record, err := storerecord.NewFromRowAndDataEntities(thisRowEntity, DataEntities)
			if err != nil {
				return nil, err
			}
			delete(dataEntitiesByKeyRev, dataKey)
			records = append(records, record)
		}

		if forceSort {
			sort.SliceStable(records, func(left, right int) bool {
				return records[left].ModRevision() < records[right].ModRevision()
			})
		}
		return records, nil
	}

	return bookKeeping, toRecords

}
func (s *store) execQuery(o *storage.QueryOptions,
	metadataLevel storage.MetadataLevel,
	bookKeepingFunc entitiesBookKeepingFn,
	ToRecordsFunc entitiesToRecordsFn) ([]types.Record, error) {

	res, err := s.t.QueryEntities(consts.DefaultTimeout, metadataLevel, o)
	if err != nil {
		return nil, err
	}

	bookKeepingFunc(res.Entities)
	for {
		if res.NextLink == nil {
			break
		}

		res, err = res.NextResults(nil) // TODO: <-- is this correct??
		if err != nil {
			return nil, err
		}

		bookKeepingFunc(res.Entities)
	}
	return ToRecordsFunc()
}

func (s *store) ListForWatch(key string, startRevision int64) ([]types.Record, error) {
	tracer := otel.Tracer("london")
	_, span := tracer.Start(context.TODO(), "listwatch")
	defer span.End()

	validKey := storerecord.CreateValidKey(key)
	validRevision := storerecord.RevToString(startRevision)
	// build a filter string for
	// current records (either newly inserted or the result of an update)
	// deleted records

	// deleted and rev >= rev
	// ||
	// current and rev >= rev

	bookKeeper, recordsMaker := s.getDefaultBookingKeepingFuncs(true)
	f := filterutils.NewFilter()
	f.And(
		filterutils.PartitionKeyPrefix(validKey),
		filterutils.GreaterThanOrEqual(consts.RevisionFieldName, validRevision),
		filterutils.ExcludeRows(),
		filterutils.CombineOr(
			//			filterutils.DeletedKeysOnly(),
			//		filterutils.CurrentKeysOnly(),
			/* this should not return any un-needed data since we also have the revision predicate */
			filterutils.IncludeDataRowsForAny(),
			filterutils.IncludeEvents(),
		),
	)
	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}

	records, err := s.execQuery(o, storage.MinimalMetadata, bookKeeper, recordsMaker)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (s *store) ListForPrefix(key string) (int64, []types.Record, error) {
	tracer := otel.Tracer("london")
	_, span := tracer.Start(context.TODO(), "listprefix")
	defer span.End()

	validKey := storerecord.CreateValidKey(key)
	bookKeeper, recordsMaker := s.getDefaultBookingKeepingFuncs(false)

	f := filterutils.NewFilter()
	f.And(
		filterutils.PartitionKeyPrefix(validKey),
		filterutils.CurrentKeysOnly(),
		filterutils.ExcludeEvents(),
	)
	f.Or(
		filterutils.CombineAnd(
			filterutils.PartitionKeyPrefix(validKey),
			filterutils.ExcludeEvents(),
			/* this potentiall returns a lot of unneeded records*/
			filterutils.IncludeDataRowsForAny(),
		),
	)

	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}
	records, err := s.execQuery(o, storage.MinimalMetadata, bookKeeper, recordsMaker)
	if err != nil {
		return 0, nil, err
	}

	currentRev, err := s.rev.Current()
	if err != nil {
		return 0, nil, err
	}

	return currentRev, records, nil
}

// ListAllCurrent lists all current keys
func (s *store) ListAllCurrent() (int64, []types.Record, error) {
	tracer := otel.Tracer("london")
	_, span := tracer.Start(context.TODO(), "listcurrent")
	defer span.End()

	// rKey == current && revision
	// This query returns only row entities
	f := filterutils.NewFilter()
	f.And(
		filterutils.CurrentKeysOnly(),
		filterutils.ExcludeEvents(),
		filterutils.ExcludeSysRecords(),
	)

	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}

	keptEntities := []*storage.Entity{}

	bookKeepingFunc := func(entities []*storage.Entity) {
		keptEntities = append(keptEntities, entities...)
	}

	entitiesToRecordsFunc := func() ([]types.Record, error) {
		records := make([]types.Record, 0, len(keptEntities))
		for _, e := range keptEntities {
			record, _, err := s.GetIfNeeded(e, true)
			if err != nil {
				return nil, err
			}
			records = append(records, record)
		}
		return records, nil
	}

	records, err := s.execQuery(o, storage.MinimalMetadata, bookKeepingFunc, entitiesToRecordsFunc)
	if err != nil {
		return 0, nil, err
	}
	currentRev, err := s.rev.Current()
	if err != nil {
		return 0, nil, err
	}

	return currentRev, records, nil
}
