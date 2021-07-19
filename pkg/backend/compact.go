package backend

import (
	"time"

	klogv2 "k8s.io/klog/v2"

	"github.com/Azure/azure-sdk-for-go/storage"

	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/backend/utils"
)

// this compacts the storage by removing all (non current keys) with rev less than rev
// in order to minimize the total calls this func produce we batch based on partition keys
// keeping each batch at max of 99 (azure storage has a 100 max entries per batch)
func (s *store) Compact(rev int64) (int64, error) {
	// only if requested rev is > than last compact
	lastCompactRev, getErr := s.GetCompactedRev(true)
	if getErr != nil {
		currentRev, _ := s.rev.Current()
		return currentRev, getErr
	}

	if lastCompactRev >= rev {
		currentRev, _ := s.rev.Current()
		return currentRev, nil
	}

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

	// set the new watermark
	err := s.setCompactedRev(rev)

	// we will attempt to get current rev.
	// but should be safe to ignore errors here
	currentRev, _ := s.rev.Current()
	return currentRev, err
}

func (s *store) GetCompactedRev(force bool) (int64, error) {
	s.lowWatermarkLock.Lock()
	defer s.lowWatermarkLock.Unlock()
	if err := s.loadCompactedRev(force); err != nil {
		return 0, err
	}

	if s.compactEntity == nil {
		return 0, nil
	}
	revString := s.compactEntity.Properties[consts.CompactRevFieldName].(string)
	currentRev := storerecord.StringToRev(revString)

	return currentRev, nil
}
func (s *store) setCompactedRev(rev int64) error {
	s.lowWatermarkLock.Lock()
	defer s.lowWatermarkLock.Unlock()

	revString := storerecord.RevToString(rev)

	if s.compactEntity == nil {
		s.compactEntity = &storage.Entity{}
		s.compactEntity.PartitionKey = consts.CompactPartitionName
		s.compactEntity.RowKey = consts.CompactRowKey
		s.compactEntity.Properties = map[string]interface{}{
			consts.CompactRevFieldName: revString,
		}
	} else {
		// only if we are moving the lowwatermark up
		revString := s.compactEntity.Properties[consts.CompactRevFieldName].(string)
		currentRev := storerecord.StringToRev(revString)
		if currentRev > rev {
			return nil
		}

		s.compactEntity.Properties = map[string]interface{}{
			consts.CompactRevFieldName: revString,
		}
	}

	batch := s.t.NewBatch()
	s.compactEntity.Table = s.t

	batch.InsertOrMergeEntity(s.compactEntity, false)
	err := utils.SafeExecuteBatch(batch)

	if storageerrors.IsEntityAlreadyExists(err) || storageerrors.IsConflictError(err) {
		err = s.loadCompactedRev(true)
		if err != nil {
			return s.setCompactedRev(rev)
		}
	}

	return err
}

// must be called with lock held
func (s *store) loadCompactedRev(force bool) error {
	if !force && s.compactEntity != nil {
		return nil // no need to load
	}
	entity := s.t.GetEntityReference(consts.CompactPartitionName, consts.CompactRowKey)
	err := utils.SafeExecuteEntityGet(entity, consts.DefaultTimeout, storage.FullMetadata, &storage.GetEntityOptions{})

	if err != nil {
		if storageerrors.IsNotFoundError(err) {
			return nil // that is fine. no compactation happened yet
		}
		return err
	}

	// got it
	s.compactEntity = entity
	return nil
}
