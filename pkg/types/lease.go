package types

type LeaseStatus int

const (
	UnknownLeaseStatus LeaseStatus = 0
	ActiveLease        LeaseStatus = 1
	RevokedLease       LeaseStatus = 2
)

type Lease struct {
	ID         int64
	Status     LeaseStatus
	GrantedTTL int64
	TTL        int64
}
