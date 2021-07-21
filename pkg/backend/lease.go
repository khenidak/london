package backend

import (
	"fmt"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	"github.com/khenidak/london/pkg/types"
)

func (s *store) CreateLease(lease *types.Lease) error {
	leaseEntity := &storage.Entity{}
	leaseEntity.PartitionKey = consts.LeasePartitionName
	leaseEntity.RowKey = fmt.Sprintf("%v", lease.ID)
	leaseEntity.Properties = map[string]interface{}{
		consts.LeaseGrantedTTLFieldName: fmt.Sprintf("%v", lease.GrantedTTL),
		consts.LeaseExpiresOnFieldName:  fmt.Sprintf("%v", lease.TTL),
		consts.LeaseStatusFieldName:     fmt.Sprintf("%v", lease.Status),
		consts.EntityTypeFieldName:      consts.EntityTypeLease,
	}

	batch := s.t.NewBatch()
	leaseEntity.Table = s.t

	batch.InsertEntity(leaseEntity)

	return batch.ExecuteBatch()
}

func (s *store) GetLease(id int64) (*types.Lease, error) {
	entity, err := s.getLeaseEntity(id)
	if err != nil {
		return nil, err
	}
	return entityToLease(entity), nil
}
func (s *store) UpdateLease(lease *types.Lease) error {
	entity, err := s.getLeaseEntity(lease.ID)
	if err != nil {
		return err
	}

	// set status if it is *not* marked as unknown
	if lease.Status != types.UnknownLeaseStatus {
		entity.Properties[consts.LeaseStatusFieldName] = fmt.Sprintf("%v", lease.Status)
	}

	// apply new ttl
	entity.Properties[consts.LeaseExpiresOnFieldName] = fmt.Sprintf("%v", lease.TTL)

	batch := s.t.NewBatch()
	batch.InsertOrMergeEntityByForce(entity) // we override existing anyway
	return batch.ExecuteBatch()
}

func (s *store) GetLeases() ([]*types.Lease, error) {
	leases := make([]*types.Lease, 0)

	fnAddLeases := func(entities []*storage.Entity) {
		for _, e := range entities {
			leases = append(leases, entityToLease(e))
		}
	}

	f := filterutils.NewFilter()
	f.And(
		filterutils.Leases(),
	)

	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}

	res, err := s.t.QueryEntities(consts.DefaultTimeout, storage.MinimalMetadata, o)
	if err != nil {
		return nil, err
	}

	if len(res.Entities) == 0 {
		return leases, nil
	}

	fnAddLeases(res.Entities)
	for {
		if res.NextLink == nil {
			break
		}

		res, err = res.NextResults(nil)
		if err != nil {
			return nil, err
		}

		if len(res.Entities) == 0 {
			return leases, nil
		}
		fnAddLeases(res.Entities)
	}
	return leases, nil
}

func (s *store) DeleteLease(id int64) error {
	entity, err := s.getLeaseEntity(id)
	if err != nil {
		return err
	}
	batch := s.t.NewBatch()
	batch.DeleteEntity(entity, true)

	return batch.ExecuteBatch()
}

func entityToLease(e *storage.Entity) *types.Lease {
	id, _ := strconv.ParseInt(e.RowKey, 10, 64)
	grantedTTL, _ := strconv.ParseInt(e.Properties[consts.LeaseGrantedTTLFieldName].(string), 10, 64)
	status, _ := strconv.ParseInt(e.Properties[consts.LeaseStatusFieldName].(string), 0, 32)
	ttl, _ := strconv.ParseInt(e.Properties[consts.LeaseExpiresOnFieldName].(string), 0, 64)

	lease := &types.Lease{
		ID:         id,
		GrantedTTL: grantedTTL,
		Status:     types.LeaseStatus(status),
		TTL:        ttl,
	}
	return lease
}

func (s *store) getLeaseEntity(id int64) (*storage.Entity, error) {
	sId := fmt.Sprintf("%v", id)
	f := filterutils.NewFilter()
	f.And(
		filterutils.LeaseId(sId),
	)
	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}

	res, err := s.t.QueryEntities(consts.DefaultTimeout, storage.FullMetadata, o)
	if err != nil {
		return nil, err
	}

	if len(res.Entities) == 0 {
		// make 404 error
		return nil, storage.AzureStorageServiceError{StatusCode: 404}
	}

	return res.Entities[0], nil
}
