package frontend

import (
	"context"
	"fmt"
	"os"
	"time"

	klogv2 "k8s.io/klog/v2"

	"go.etcd.io/etcd/etcdserver/etcdserverpb"

	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"
	"github.com/khenidak/london/pkg/types"
)

const (
	leaseManagementElectionName = "lease-manager"
)

func (fe *frontend) leaseMangementLoop() {
	var leader types.LeaderElect
	myName, err := os.Hostname()
	if err != nil {
		// let us generate a random name
		myName := fmt.Sprintf("%v", time.Now().UTC().UnixNano())
		klogv2.Infof("lease manager: failed to get hostname with err:%v and used a random generated name instead:%v - process restart will yield in lease loss", err, myName)
	}
	var ticker *time.Ticker
	if fe.config.LeaseMgmtRunInterval != 0 {
		klogv2.Infof("lease manager: operating at test 1s intervals")
		ticker = time.NewTicker(time.Second * time.Duration(fe.config.LeaseMgmtRunInterval))
	} else {
		klogv2.Infof("lease manager: operating at 10s intervals")
		ticker = time.NewTicker(time.Second * 10)
	}

	stopCh := fe.config.Runtime.Context.Done()
	for {
		select {
		case <-stopCh:
			// done here
			return
		case <-ticker.C:
			if leader == nil {
				leader = fe.be.NewLeaderElection(leaseManagementElectionName, myName)
				elected, err := leader.Elect(time.Second * 10)
				if err != nil {
					klogv2.Infof("lease manager:encountered an error trying to elect leader:%v err:%v .. will retry in 10s", err)
					continue
				}

				if !elected {
					holder, _ := leader.CurrentHolder()
					klogv2.Infof("lease manager: failed to elect leader. current leader is:%v.. will try again in 10s", holder)
					continue
				}
				// we are leader for at least 10s
				// let us do the work. We are not passing
				// context. assuming that whatever # of expired
				// leases will be processed in < 10s window
				fe.leaseManagementIteration()
			}
		}
	}
}

func (fe *frontend) leaseManagementIteration() {
	// get all current leases

	// The docs is not clear on what happens to
	// Update/Delete ops on keys with attached lease.
	// for now we are DELETE all keys. That means
	// if a key  was updated with a lease it will be
	// DELETED-- not reverted back to original value
	leases, err := fe.be.GetLeases()
	if err != nil {
		klogv2.Infof("lease manager: failed to get leases with err:%v will try again in 5s", err)
	}

	// leases that have expired.
	leasesToOperateOn := make([]*types.Lease, 0, 0)

	for _, lease := range leases {
		if lease.Status == types.RevokedLease || hasExpired(lease) {
			leasesToOperateOn = append(leasesToOperateOn, lease)
		}
	}

	klogv2.Infof("lease manager: number of expired leases:%v", len(leasesToOperateOn))
	// in this list, if a lease is active
	// expire it and then delete its data
	// if it was revoked check that data is
	// removed then
	for _, lease := range leasesToOperateOn {
		if lease.Status == types.ActiveLease {
			// mark it as in active
			lease.Status = types.RevokedLease
			if err := fe.be.UpdateLease(lease); err != nil {
				klogv2.Infof("lease manager: failed to mark lease:%v as revoked with err:%v will try again later", lease.ID, err)
				continue
			}
			klogv2.Infof("lease manager: marked lease:%v as revoked because it has expired. will delete related keys", lease.ID)
		}
		_, records, err := fe.be.ListAllCurrentWithLease(lease.ID)
		if err != nil {
			klogv2.Infof("lease manager: failed to get records for lease:%v err:%v, will try again later", lease.ID, err)
			continue
		}

		if len(records) == 0 {
			// this is a candidate for removal
			if err := fe.be.DeleteLease(lease.ID); err != nil {
				klogv2.Infof("lease manager: failed to delete lease:%v err:%v will try agaion later", lease.ID, err)
				continue
			}
			klogv2.Infof("lease manager: deleted expired lease:%v", lease.ID)
		}

		// delete all records assiated with this lease
		for _, r := range records {
			_, err := fe.be.Delete(string(r.Key()), r.ModRevision())
			if err != nil {
				klogv2.Infof("lease manager: failed to delete key(%v):[%v]:%v err:%v will try again later", lease.ID, r.ModRevision(), string(r.Key()), err)
				continue
			}
			klogv2.Infof("lease manager: deleted key(%v):[%v]:%v", string(r.Key()), lease.ID, r.ModRevision())
		}
	}
}
func hasExpired(lease *types.Lease) bool {
	now := time.Now().UTC().Unix()
	return lease.TTL <= now
}

/* Lease service does not seem to be used by kubernetes */
func (fe *frontend) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {

	// set id to some int64 if we are requested to generate the id
	now := time.Now().UTC()
	id := req.ID
	if id == 0 {
		id = now.UnixNano()
	}

	// calc the ttl
	ttl := now.Unix() + req.TTL

	newLease := &types.Lease{
		ID:         id,
		Status:     types.ActiveLease,
		GrantedTTL: req.TTL,
		TTL:        ttl,
	}

	err := fe.be.CreateLease(newLease)
	currentRev, _ := fe.be.CurrentRevision()
	response := &etcdserverpb.LeaseGrantResponse{
		Header: createResponseHeader(currentRev),
		TTL:    req.TTL,
		ID:     newLease.ID,
	}

	return response, err
}

func (fe *frontend) LeaseRevoke(context context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	currentLease := &types.Lease{
		ID:     req.ID,
		Status: types.RevokedLease,
	}
	err := fe.be.UpdateLease(currentLease)

	currentRev, _ := fe.be.CurrentRevision()
	response := &etcdserverpb.LeaseRevokeResponse{
		Header: createResponseHeader(currentRev),
	}
	return response, err
}

func (fe *frontend) LeaseTimeToLive(context context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {

	if req.Keys {
		return nil, fmt.Errorf("Keys for lease is not supported")
	}
	lease, err := fe.be.GetLease(req.ID)

	if err != nil {
		if storageerrors.IsNotFoundError(err) {
			return nil, nil
		}
		// error we can not deal with
		return nil, err
	}

	now := time.Now().UTC().Unix()
	ttl := int64(0)
	if lease.TTL > now {
		ttl = lease.TTL - now
	} else {
		ttl = 0
	}

	currentRev, _ := fe.be.CurrentRevision()

	response := &etcdserverpb.LeaseTimeToLiveResponse{
		Header:     createResponseHeader(currentRev),
		TTL:        ttl,
		GrantedTTL: lease.GrantedTTL,
	}

	return response, nil
}
func (fe *frontend) LeaseLeases(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {

	leases, err := fe.be.GetLeases()
	if err != nil {
		return nil, err
	}
	currentRev, _ := fe.be.CurrentRevision()

	response := &etcdserverpb.LeaseLeasesResponse{
		Header: createResponseHeader(currentRev),
		Leases: make([]*etcdserverpb.LeaseStatus, 0, len(leases)),
	}
	for _, lease := range leases {
		if lease.Status != types.RevokedLease {
			response.Leases = append(response.Leases, &etcdserverpb.LeaseStatus{ID: lease.ID})
		}
	}

	return response, nil
}

func (fe *frontend) LeaseKeepAlive(srv etcdserverpb.Lease_LeaseKeepAliveServer) error {
	go fe.leaseLoop(srv)
	return nil
}

func (fe *frontend) leaseLoop(srv etcdserverpb.Lease_LeaseKeepAliveServer) {
	done := srv.Context().Done()

	for {
		select {

		case <-done:
			return

		default:
			req, err := srv.Recv()
			if err != nil {
				klogv2.Infof("error recieve from keep alive server %v", err)
				continue
			}
			now := time.Now().UTC()
			id := req.ID
			ttl := now.Unix() + 10 //10 sec default

			currentLease := &types.Lease{
				ID:     id,
				Status: types.ActiveLease,
				TTL:    ttl,
			}

			errUpdate := fe.be.UpdateLease(currentLease)
			currentRev, _ := fe.be.CurrentRevision()

			response := &etcdserverpb.LeaseKeepAliveResponse{
				Header: createResponseHeader(currentRev),
				ID:     id,
				TTL:    10, // default
			}
			if errUpdate != nil {
				klogv2.Infof("unable to perform lease keep a life for:%v with error:%v", id, errUpdate)
				response.TTL = 0
			}

			// we tell sender the results anyway.
			if errSend := srv.Send(response); errSend != nil {
				// something is wrong with sending on this server
				// collapse the entire server
				klogv2.Infof("failed to send response for lease keep alive for ID:%v with err:%v", id, errSend)
				continue
			}
		}
	}
}
