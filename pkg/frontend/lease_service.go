package frontend

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/etcdserver/etcdserverpb"
)

/* Lease service does not seem to be used by kubernetes */
func (fe *frontend) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	return nil, fmt.Errorf("lease grant is not supported")
}

func (fe *frontend) LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return nil, fmt.Errorf("lease revoke is not supported")
}

func (fe *frontend) LeaseKeepAlive(etcdserverpb.Lease_LeaseKeepAliveServer) error {
	return fmt.Errorf("lease keep alive is not supported")
}

func (fe *frontend) LeaseTimeToLive(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return nil, fmt.Errorf("lease time to live is not supported")
}
func (fe *frontend) LeaseLeases(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return nil, fmt.Errorf("lease leases is not supported")
}
