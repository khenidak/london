package backend

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	"github.com/khenidak/london/pkg/backend/revision"
	"github.com/khenidak/london/pkg/backend/utils"
	"github.com/khenidak/london/pkg/config"
	"github.com/khenidak/london/pkg/types"
)

/*
	a keyvalue pair are saved as:
	1. row entity (carries main details +640KB split over 10 props each 64k)
	2. n data entities. Each is 640KB split over 10 props
	This enables support > 1mb limits of Azure table.

	Each record carries
	- CreateRevision int64 -- assigned once on Insert for new record CreateRev and ModRev (read below)
	  are the same
	- ModRevision int64 used as RowKey in all cases except (current value, described below). ModRev is updated
	everytime we update/delete the record. CreateRev is kept the same
	- Update actions updates PrevRev field to allow getting old/new for watchers

	a "current record" will always carry a known value in RowKey. This to ensure that an
	insert will fail if there is a current record already exists.

	each action on record creates an event. An event is one row entity (with no data entities, point to existing  data entities that are created by the record modification). Each event carries the same ModRev as the action  that caused the event to be created. Watch is all about these events.

	When Inserting rowKey == fixed value
	When deleting we create a new row  mark it as delete
	When updating we create replace existing current with new value and copy the previous record into a new reco  rd marking it as update. The existing current will carry PrevRevision

	*** all entities maintain a well known field for revision
*/

type Backend interface {
	Insert(key string, value []byte, lease int64) (int64, error)
	Get(key string, revision int64) (types.Record, int64, error)
	Delete(key string, revision int64) (types.Record, error)
	Update(key string, val []byte, revision int64, lease int64) (types.Record, error)
	ListAllCurrent() (int64, []types.Record, error)
	ListForPrefix(prefix string) (int64, []types.Record, error)
	ListForWatch(key string, startRevision int64) ([]types.Record, error)
	DeleteAllBeforeRev(rev int64) (int64, error)
	CurrentRevision() (int64, error)
}

type store struct {
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
