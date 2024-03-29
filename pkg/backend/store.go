package backend

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	"github.com/khenidak/london/pkg/backend/revision"
	"github.com/khenidak/london/pkg/backend/utils"
	"github.com/khenidak/london/pkg/config"
	"github.com/khenidak/london/pkg/types"
)

/*
	a keyvalue pair are saved as a record. Each Record is:
	1. row entity (carries main details +640KB split over 10 props each 64k)
	2. n data entities. Each is 640KB split over 10 props
	This enables support > 1mb limits of Azure table.

	Each record carries
	- CreateRevision int64 -- assigned once on Insert for new record CreateRev and ModRev (read below)
	  are the same
	- ModRevision int64 used as RowKey in all cases except (current value, described below). ModRev is updated
	everytime we update/delete the record. CreateRev is kept the same
	- Update actions updates PrevRev field to allow getting old/new for watchers

	a "current record" will always carry a known value (we use "C") in RowKey. This to ensure that an
	insert will fail if there is a current record already exists.

	each action on record creates an event. An event is one row entity. Each event carries the same ModRev as the action  that caused the event to be created. Watch is all about these events.

	// TODO (@khenidak): A major optimization is to relay only on events for prev value. That means:
  // on create: Create Record + Event
	// On update: create the record, delete old record + create Event
	// On Delete: Deleted old record + Create Event
  // Note the optimization have impact on total # of api calls per operation
	// But has smaller entities per batch.

	Ops:
	When Inserting Record.RowKey == fixed value
	When deleting we delete existing Record.RowKey == fixed + Create a new record mark it as deleted
	When updating we create replace existing current with new value and copy the previous record into a new record marking it as update. The existing current will carry PrevRevision

	*** all entities maintain a well known field for revision
*/

type Backend interface {
	Insert(key string, value []byte, lease int64) (types.Record, error)
	Get(key string, revision int64) (types.Record, int64, error)
	Delete(key string, revision int64) (types.Record, error)
	Update(key string, val []byte, revision int64, lease int64) (types.Record, types.Record, error)
	ListAllCurrent() (int64, []types.Record, error)
	ListForPrefix(prefix string) (int64, []types.Record, error)
	ListEvents(startRevision int64) ([]types.Record, error)
	Compact(rev int64) (int64, error)
	GetCompactedRev(force bool) (int64, error)
	CurrentRevision() (int64, error)

	// leader election stuff
	NewLeaderElection(electionName string, myName string) types.LeaderElect

	// lease stuff
	ListAllCurrentWithLease(leaseId int64) (int64, []types.Record, error)
	CreateLease(lease *types.Lease) error
	DeleteLease(id int64) error
	UpdateLease(lease *types.Lease) error
	GetLease(id int64) (*types.Lease, error)
	GetLeases() ([]*types.Lease, error)
}

type store struct {
	// last compacted
	lowWatermarkLock sync.Mutex
	compactEntity    *storage.Entity

	rev    revision.Revisioner
	config *config.Config
	t      *storage.Table
}

func NewBackend(c *config.Config) (Backend, error) {
	revisioner, err := revision.NewRevisioner(c.Runtime.RevisionStorageTable)
	if err != nil {
		return nil, err
	}

	s := &store{
		config: c,
		rev:    revisioner,
		t:      c.Runtime.StorageTable,
	}

	if err := s.ensureStore(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *store) CurrentRevision() (int64, error) {
	return s.rev.Current()
}

func (s *store) ensureStore() error {
	// TODO -- should we auto create the table?
	err := s.t.Create(100, storage.EmptyPayload, &storage.TableOptions{})
	if err != nil {
		var status storage.AzureStorageServiceError
		if !errors.As(err, &status) {
			return err
		}

		if status.StatusCode != http.StatusConflict {
			return fmt.Errorf("got status code %d:  %v", status.StatusCode, err)
		}
	}
	// Test that we have write access
	e := &storage.Entity{
		Table: s.t,
	}
	e.PartitionKey = consts.WriteTesterPartitionKey
	e.RowKey = consts.WriteTestRowKey
	e.Properties = map[string]interface{}{
		"what_is_this": "we use it to test that keys/connection/sas/whatever allow write access",
		"when":         time.Now().UTC().String(),
	}

	b := s.t.NewBatch()
	b.Table = s.t

	b.InsertOrMergeEntity(e, true)
	return utils.SafeExecuteBatch(b)
}

// storage may return empty res with NextLink to follow. The helper
// works around that
func (s *store) execQueryFull(metadataLevel storage.MetadataLevel, o *storage.QueryOptions) ([]*storage.Entity, error) {
	all := make([]*storage.Entity, 0, 16) // rough # of large record
	res, err := utils.SafeExecuteQuery(s.t, consts.DefaultTimeout, metadataLevel, o)
	if err != nil {
		return nil, err
	}

	all = append(all, res.Entities...)

	for {
		if res.NextLink == nil {
			break
		}

		res, err = utils.SafeExecuteNextResult(res, nil)
		if err != nil {
			return nil, err
		}
		all = append(all, res.Entities...)
	}

	return all, nil
}
