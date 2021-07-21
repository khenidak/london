package backend

import (
	"fmt"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/backend/utils"
	"github.com/khenidak/london/pkg/types"
)

func (s *store) Get(key string, revision int64) (types.Record, int64, error) {
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

	entities, err := s.execQueryFull(storage.MinimalMetadata, o)
	if err != nil {
		return nil, 0, err
	}

	currentRev, _ := s.rev.Current()
	// no results or deleted record
	if len(entities) == 0 {
		return nil, currentRev, nil
	}

	if !getCurrent {
		// this is get by Revision
		// we need another query, since the original query included the data
		record, err := storerecord.NewFromEntities(entities, false)
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
	if len(entities) > 1 {
		return nil, currentRev, fmt.Errorf("expected to get one current row got %v", len(entities))
	}

	// get data for this row
	rowRev := entities[0].Properties[consts.RevisionFieldName].(string)
	dataFilter := filterutils.NewFilter()
	dataFilter.And(
		filterutils.IncludeDataRows(rowRev),
	)

	o.Filter = dataFilter.Generate()
	//	klogv2.Infof("GET-DATA-ROWS-FOR-LATEST:%v", o.Filter)
	dataRes, err := utils.SafeExecuteQuery(s.t, consts.DefaultTimeout, storage.MinimalMetadata, o)
	if err != nil {
		return nil, currentRev, err
	}

	record, err := storerecord.NewFromRowAndDataEntities(entities[0], dataRes.Entities)
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
