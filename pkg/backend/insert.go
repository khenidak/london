package backend

import (
	klogv2 "k8s.io/klog/v2"

	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"
	"github.com/khenidak/london/pkg/backend/storerecord"
	"github.com/khenidak/london/pkg/backend/utils"
)

// Insert perfroms create. by creating a new record and marking it as current
// The only way we can ensure that there is no "current" record is by using
// rowKey == C and partition key is the same
func (s *store) Insert(key string, value []byte, lease int64) (int64, error) {
	klogv2.Infof("STORE-INSERT: %v", key)
	//	validKey := storerecord.CreateValidKey(key)
	rev, err := s.rev.Increment()
	if err != nil {
		return 0, err
	}

	record, err := storerecord.NewRecord(key, rev, lease, value)
	if err != nil {
		return 0, err
	}

	event := storerecord.CreateEventEntityFromRecord(record)

	batch := s.t.NewBatch()
	rowEntity := record.RowEntity()
	rowEntity.Table = s.t
	batch.InsertEntity(rowEntity)

	// batch data entities in one go
	dataEntities := record.DataEntities()
	for _, de := range dataEntities {
		de.Table = s.t
		batch.InsertEntity(de)
	}

	// insert the event
	event.Table = s.t
	batch.InsertEntity(event)

	err = utils.SafeExecuteBatch(batch)

	if storageerrors.IsEntityAlreadyExists(err) {
		// get current entity and return rev
		// Kubernetes does not really use that rev
		// but we do that just in case
		_, rev, _ := s.Get(key, 0)
		return rev, err // <- make sure that AlreadyExistError is the one returned
	}
	return rev, err
}
