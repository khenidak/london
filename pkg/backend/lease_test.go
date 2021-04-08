package backend

import (
	"testing"
	"time"

	storageerrors "github.com/khenidak/london/pkg/backend/storageerrors"
	"github.com/khenidak/london/pkg/types"

	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func TestCreateGetLease(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	id := time.Now().UTC().UnixNano()
	ttl := id + 10
	gttl := ttl
	status := types.ActiveLease

	lease := &types.Lease{
		ID:         id,
		TTL:        ttl,
		GrantedTTL: gttl,
		Status:     status,
	}

	err = be.CreateLease(lease)
	if err != nil {
		t.Fatalf("failed to create lease with err:%v", err)
	}

	// get
	gotLease, err := be.GetLease(id)
	if err != nil {
		t.Fatalf("failed to get lease with err:%v", err)
	}

	leaseAreEqual(lease, gotLease, t, true)
}

func TestDeleteLease(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	id := time.Now().UTC().UnixNano()
	ttl := id + 10
	gttl := ttl
	status := types.ActiveLease

	lease := &types.Lease{
		ID:         id,
		TTL:        ttl,
		GrantedTTL: gttl,
		Status:     status,
	}

	err = be.CreateLease(lease)
	if err != nil {
		t.Fatalf("failed to create lease with err:%v", err)
	}

	// delete it
	err = be.DeleteLease(lease.ID)
	if err != nil {
		t.Fatalf("failed to delete lease with err:%v", err)
	}

	// get
	_, err = be.GetLease(id)
	if err == nil {
		t.Fatalf("expected an error when getting a delete lease, got nothing")
	}

	if !storageerrors.IsNotFoundError(err) {
		t.Fatalf("expected not found error while getting a deleted lease got:%v", err)
	}
}

func TestUpdateLease(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	id := time.Now().UTC().UnixNano()
	ttl := id + 10
	gttl := ttl
	status := types.ActiveLease

	lease := &types.Lease{
		ID:         id,
		TTL:        ttl,
		GrantedTTL: gttl,
		Status:     status,
	}

	err = be.CreateLease(lease)
	if err != nil {
		t.Fatalf("failed to create lease with err:%v", err)
	}

	// get
	gotLease, err := be.GetLease(id)
	if err != nil {
		t.Fatalf("failed to get lease with err:%v", err)
	}

	// update it
	gotLease.Status = types.RevokedLease
	gotLease.TTL = gotLease.TTL + 1000

	err = be.UpdateLease(gotLease)
	if err != nil {
		t.Fatalf("failed tp update lease with err:%v", err)
	}

	// get again
	updatedLease, err := be.GetLease(id)
	if err != nil {
		t.Fatalf("failed to get lease with err:%v", err)
	}

	leaseAreEqual(gotLease, updatedLease, t, true)
}

func TestGetLeases(t *testing.T) {
	// because we don't want to perform clean table
	// we do testing in this weird way.
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	savedLeases := make([]*types.Lease, 0)
	for i := 1; i < 5; i++ {
		id := time.Now().UTC().UnixNano()
		ttl := id + 10
		gttl := ttl
		status := types.ActiveLease

		lease := &types.Lease{
			ID:         id,
			TTL:        ttl,
			GrantedTTL: gttl,
			Status:     status,
		}
		err = be.CreateLease(lease)
		if err != nil {
			t.Fatalf("failed to create lease with err:%v", err)
		}

		savedLeases = append(savedLeases, lease)

	}

	leases, err := be.GetLeases()
	if err != nil {
		t.Fatalf("failed to list leases with err:%v", err)
	}

	foundLeases := make([]*types.Lease, 0, len(savedLeases))
	for _, serverLease := range leases {
		for _, savedLease := range savedLeases {
			if leaseAreEqual(savedLease, serverLease, t, false) {
				foundLeases = append(foundLeases, savedLease)
			}
		}
	}

	if len(foundLeases) != len(savedLeases) {
		t.Fatalf("failed to find all leases expected:%+v\ngot:%+v", savedLeases, leases)
	}
}

func leaseAreEqual(src, tgt *types.Lease, t *testing.T, fail bool) bool {
	t.Helper()

	if tgt.ID != src.ID {
		if fail {
			t.Fatalf("expected lease id:%v got %v", src.ID, tgt.ID)
		}
	}

	if tgt.TTL != src.TTL {
		if fail {
			t.Fatalf("expected ttl:%v got %v", src.TTL, tgt.TTL)
		} else {
			return false
		}
	}

	if tgt.GrantedTTL != src.GrantedTTL {
		if fail {
			t.Fatalf("expected granted ttl:%v got %v", src.GrantedTTL, tgt.GrantedTTL)
		} else {
			return false
		}
	}

	if tgt.Status != tgt.Status {
		if fail {
			t.Fatalf("expected granted Status:%v got %v", src.Status, tgt.Status)
		} else {
			return false
		}
	}

	return true
}
