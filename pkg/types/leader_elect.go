package types

import "time"

type LeaderElect interface {
	ElectionName() string
	MyName() string
	Elect(duration time.Duration) (bool, error)
	CurrentHolder() (string, error)
	ExpiresOn() (*time.Time, error)
	RenewTerm(duration time.Duration) (bool, error)
}
