package backend

import (
	"fmt"
	"sync"
	"time"

	klogv2 "k8s.io/klog/v2"

	"github.com/Azure/azure-sdk-for-go/storage"

	"github.com/khenidak/london/pkg/backend/consts"
	filterutils "github.com/khenidak/london/pkg/backend/filter"
	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"
	"github.com/khenidak/london/pkg/types"
)

/* We are not doing a traditional leader-elect here. The primary difference being the caller
is responsible for renewing the leader-elect-lease. We are not running a go-routine on behalf of caller and renewing it.
// TODO: do we need a release api?

*/

type le struct {
	lock sync.Mutex
	// election name
	electionName string
	// my name is current process, host whatever competes for the election
	myName string

	s *store
	e *storage.Entity
}

// creates new election based on storage table
func (s *store) NewLeaderElection(electionName string, myName string) types.LeaderElect {
	// note we are not testing names here. Thus user can create empty electionName and myName
	return &le{
		electionName: electionName,
		myName:       myName,
		s:            s,
	}
}

func (l *le) ElectionName() string {
	return l.electionName
}

func (l *le) MyName() string {
	return l.myName
}

// elects current leader
func (l *le) Elect(duration time.Duration) (bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	// first try to blindly insert a new entity
	e := createElectionEntity(l, duration)
	e.Table = l.s.t // assigns the table
	batch := l.s.t.NewBatch()
	batch.InsertEntity(e)
	err := batch.ExecuteBatch()

	// assuming that we are clear
	if err == nil {
		// optionally save entity
		_ = l.ensureCurrentEntity()
		klogv2.Infof("leader election:%v is currently owned by:%v", l.electionName, l.myName)
		return true, nil
	}

	if !storageerrors.IsEntityAlreadyExists(err) {
		// this is not an error we can deal with
		return false, err
	}

	return l.renewTerm(duration)
}

func (l *le) CurrentHolder() (string, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if err := l.ensureCurrentEntity(); err != nil {
		return "", err
	}

	return l.e.Properties[consts.LeaderElectOwnerNameFieldName].(string), nil
}

func (l *le) RenewTerm(duration time.Duration) (bool, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	return l.renewTerm(duration)
}
func (l *le) renewTerm(duration time.Duration) (bool, error) {
	if err := l.ensureCurrentEntity(); err != nil {
		return false, err
	}

	// case one: current holder is us
	currentHolder := l.e.Properties[consts.LeaderElectOwnerNameFieldName].(string)
	if currentHolder == l.myName {
		// over-write expires on
		expiresOn := time.Now().Add(duration)
		l.e.Properties[consts.LeaderElectExpiresOnFieldName] = expiresOn.Unix()
		l.e.Table = l.s.t
		batch := l.s.t.NewBatch()
		batch.MergeEntity(l.e)

		err := batch.ExecuteBatch()
		if err != nil {
			if storageerrors.IsConflictError(err) {
				l.e = nil
				// reset it and try again
				// we assume sanity across caller otherwise this will quickly
				// consume our stack
				return l.renewTerm(duration)
			}
			return false, err
		}

		// optional
		_ = l.ensureCurrentEntity()
		klogv2.Infof("leader election:%v has been renewed for owner:%v",
			l.electionName,
			l.myName)

		return true, nil
	}

	// case two: current holder is not us
	currentExpiresOnUnix := l.e.Properties[consts.LeaderElectExpiresOnFieldName].(int64)
	currentExpiresOn := time.Unix(currentExpiresOnUnix, 0)

	if currentExpiresOn.Before(time.Now().UTC()) {
		klogv2.Infof("leader election:%v has expired for owner:%v. attempting take over with new owner:%v",
			l.electionName,
			l.e.Properties[consts.LeaderElectOwnerNameFieldName],
			l.myName)
		// expired, try to take over
		expiresOn := time.Now().Add(duration)
		l.e.Properties[consts.LeaderElectExpiresOnFieldName] = expiresOn.Unix()
		l.e.Properties[consts.LeaderElectOwnerNameFieldName] = l.myName
		l.e.Table = l.s.t
		batch := l.s.t.NewBatch()
		batch.MergeEntity(l.e)

		// we really need to workout errors
		// TODO
		err := batch.ExecuteBatch()
		if err != nil {
			if storageerrors.IsConflictError(err) {
				// reset it and try again
				// we assume sanity across caller otherwise this will quickly
				// consume our stack
				l.e = nil
				return l.renewTerm(duration)
			}
			return false, err
		}
		// optional
		_ = l.ensureCurrentEntity()
		return true, nil
	}

	// unexpired. owned by a different owner
	return false, nil
}
func (l *le) ExpiresOn() (*time.Time, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if err := l.ensureCurrentEntity(); err != nil {
		return nil, err
	}
	currentExpiresOnUnix := l.e.Properties[consts.LeaderElectExpiresOnFieldName].(int64)
	expires := time.Unix(currentExpiresOnUnix, 0)
	return &expires, nil
}

func (l *le) ensureCurrentEntity() error {
	if l.e == nil {
		currentEntity, err := l.s.getElectionEntity(l.electionName)
		if err != nil {
			return err
		}
		l.e = currentEntity
	}

	return nil
}

func (s *store) getElectionEntity(electionName string) (*storage.Entity, error) {
	f := filterutils.NewFilter()

	f.And(filterutils.LeaderElectionEntity(electionName))
	o := &storage.QueryOptions{
		Filter: f.Generate(),
	}

	res, err := s.t.QueryEntities(consts.DefaultTimeout, storage.FullMetadata, o)
	if err != nil {
		return nil, err
	}

	if len(res.Entities) == 0 {
		return nil, fmt.Errorf("election:%v does not exist", electionName)
	}

	return res.Entities[0], nil // must be only one due to pkey/rkey pairing
}

func createElectionEntity(l *le, duration time.Duration) *storage.Entity {
	expiresOn := time.Now().Add(duration)

	e := &storage.Entity{}
	e.PartitionKey = consts.LeaderElectPartitionName
	e.RowKey = l.electionName
	e.Properties = map[string]interface{}{
		consts.LeaderElectOwnerNameFieldName: l.myName,
		consts.LeaderElectExpiresOnFieldName: expiresOn.Unix(),
		consts.EntityTypeFieldName:           consts.EntityTypeLeaderElection,
	}

	return e
}
