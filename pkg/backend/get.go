package backend

import (
	"context"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel"
	klogv2 "k8s.io/klog/v2"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/types"
)

func (s *store) Get(key string, revision int64) (types.Record, int64, error) {
	klogv2.Infof("STORE-GET: %v:%v", revision, key)
	tracer := otel.Tracer("london")
	_, span := tracer.Start(context.TODO(), "get")
	defer span.End()

	validKey := storerecord.CreateValidKey(key)
	validRev := storerecord.RevToString(revision)

	f := filterutils.NewFilter()
	f.And(
		filterutils.Partition(validKey),
		filterutils.ExcludeEvents(),
	)

	getCurrent := false
	if revision == 0 {
		// get current value
		getCurrent = true
		f.And(filterutils.CurrentKeysOnly())
	} else {
		// by revision (even if not current)
		f.And(
			filterutils.RevisionAny(validRev),
			filterutils.ExcludeEvents(),
		)
		f.Or(filterutils.IncludeDataRows(validRev))
	}

	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}

	res, err := s.t.QueryEntities(consts.DefaultTimeout, storage.MinimalMetadata, o)
	if err != nil {
		return nil, 0, err
	}

	currentRev, _ := s.rev.Current()
	// no results or deleted record
	if len(res.Entities) == 0 {
		return nil, currentRev, nil
	}

	if !getCurrent {
		// this is get by Revision
		// we need another query, since the original query included the data
		record, err := storerecord.NewFromEntities(res.Entities, false)
		if err != nil {
			return nil, currentRev, err
		}
		if record.ModRevision() > currentRev {
			currentRev = record.ModRevision()
		}
		return record, currentRev, err
	}

	// if we are trying to get current record
	// then res must be the row, now we need get the data
	if len(res.Entities) > 1 {
		return nil, currentRev, fmt.Errorf("expected to get one current row got %v", len(res.Entities))
	}

	// get data for this row
	rowRev := res.Entities[0].Properties[consts.RevisionFieldName].(string)
	dataFilter := filterutils.NewFilter()
	dataFilter.And(
		filterutils.IncludeDataRows(rowRev),
	)

	o.Filter = dataFilter.Generate()
	klogv2.Infof("GET-DATA-ROWS-FOR-LATEST:%v", o.Filter)
	dataRes, err := s.t.QueryEntities(consts.DefaultTimeout, storage.MinimalMetadata, o)
	if err != nil {
		return nil, currentRev, err
	}

	record, err := storerecord.NewFromRowAndDataEntities(res.Entities[0], dataRes.Entities)
	if err != nil {
		return nil, currentRev, err
	}

	if record.ModRevision() > currentRev {
		currentRev = record.ModRevision()
	}

	return record, currentRev, nil
}

// given a row entity, do we need to get data entities or can we
// create a record with only row entity?
func (s *store) GetIfNeeded(rowEntity *storage.Entity, forceCurrent bool) (types.Record, int64, error) {
	klogv2.Infof("STORE-GET-IF-NEEDED: %v:%v (current:%v)", rowEntity.Properties[consts.RevisionFieldName], rowEntity.PartitionKey, forceCurrent)
	countDataEntitiesAsString := rowEntity.Properties[consts.DataPartsCountFieldName].(string)
	countDataEntities, _ := strconv.ParseInt(countDataEntitiesAsString, 10, 32)
	if countDataEntities > 0 {
		if !forceCurrent {
			targetRev, _ := strconv.ParseInt(rowEntity.Properties[consts.RevisionFieldName].(string), 10, 64)
			return s.Get(rowEntity.PartitionKey, targetRev)
		}

		return s.Get(rowEntity.PartitionKey, 0)
	}

	currentRev, _ := s.rev.Current()
	record, err := storerecord.NewFromEntities([]*storage.Entity{rowEntity}, false)
	if err != nil {
		return nil, currentRev, err
	}
	return record, currentRev, nil

}
