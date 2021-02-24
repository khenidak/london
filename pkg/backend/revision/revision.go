package revision

import (
	"sync"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"
)

type Revisioner interface {
	Increment() (int64, error)
	Current() (int64, error)
}

type rev struct {
	mu sync.Mutex
	// storage client
	t *storage.Table
	// reference to entity that holds the revision across the entire table
	e *storage.Entity
}

func NewRevisioner(t *storage.Table) Revisioner {
	return &rev{
		t: t,
	}
}

func (r *rev) Current() (int64, error) {
	current := func() int64 {
		r.mu.Lock()
		defer r.mu.Unlock()

		if r.e != nil {
			return r.e.Properties[consts.RevisionerProperty].(int64)
		}
		return 0
	}()

	if current != 0 {
		return current, nil
	}

	return r.Increment()
}

func (r *rev) Increment() (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		var e *storage.Entity
		var err error

		if r.e == nil {
			e, err = r.getRev()
			if err != nil {
				if !storageerrors.IsNotFoundError(err) {
					return 0, err
				}

				// not found.. insert new
				e, err = r.newRev()
				if err != nil {
					if !storageerrors.IsEntityAlreadyExists(err) {
						return 0, err
					}
					// conflict error. try again
					continue
				}
			}
			r.e = e
		}

		// increase
		current := (r.e.Properties[consts.RevisionerProperty].(int64))
		current = current + 1

		r.e.Properties[consts.RevisionerProperty] = current
		err = r.updateRev()
		if err != nil {
			if storageerrors.IsConflictError(err) {
				// remove local and acquire again
				r.e = nil
				continue
			}
			return 0, err
		}

		return current, nil
	}
}

func (r *rev) newRev() (*storage.Entity, error) {
	var zero int64

	b := r.t.NewBatch()
	entity := &storage.Entity{
		Table: r.t,
	}
	entity.PartitionKey = consts.RevisionerPartitionKey
	entity.RowKey = consts.RevisionerRowKey
	entity.Properties = make(map[string]interface{})
	entity.Properties[consts.RevisionerProperty] = zero
	entity.OdataEtag = "abc"
	b.InsertEntity(entity)
	err := b.ExecuteBatch()
	if err != nil {
		return nil, err
	}

	return entity, nil
}

func (r *rev) getRev() (*storage.Entity, error) {
	entity := r.t.GetEntityReference(consts.RevisionerPartitionKey, consts.RevisionerRowKey)
	err := entity.Get(consts.DefaultTimeout, storage.FullMetadata, &storage.GetEntityOptions{Select: []string{consts.RevisionerProperty}})

	if err != nil {
		return nil, err
	}

	return entity, nil
}

func (r *rev) updateRev() error {
	b := r.t.NewBatch()
	b.InsertOrMergeEntity(r.e, false)
	return b.ExecuteBatch()
}
