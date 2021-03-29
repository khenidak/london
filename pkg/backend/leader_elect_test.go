package backend

import (
	"testing"
	"time"

	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func TestLeaderElection(t *testing.T) {
	c := basictestutils.MakeTestConfig(t, false)

	be, err := NewBackend(c)
	if err != nil {
		t.Fatalf("failed to create backend with err:%v", err)
	}

	electionName := randStringRunes(8)
	myName := randStringRunes(8)

	// test that name/myname are set correctly
	election := be.NewLeaderElection(electionName, myName)
	if election.ElectionName() != electionName {
		t.Fatalf("expected election name == %v got %v", electionName, election.ElectionName())
	}
	if election.MyName() != myName {
		t.Fatalf("expected election MY name == %v got %v", myName, election.MyName())
	}

	// we know that this election is new (we are randomizing name)
	// unless we really really messed up and generated a name that was used before
	ok, err := election.Elect(time.Second * 5)
	if err != nil {
		t.Fatalf("failed to elect self as leader with err:%v", err)
	}
	if !ok {
		t.Fatalf("expected election to successed but it didn't")
	}

	holderName, err := election.CurrentHolder()
	if err != nil {
		t.Fatalf("failed to get current holder with error: %v", err)
	}

	if holderName != myName {
		t.Fatalf("expected current holder to be %v got %v", myName, holderName)
	}

	// try creating a new election with same name.. different myName
	sameElectionDifferentOwner := be.NewLeaderElection(electionName, "not-the-same-owner")
	ok, err = sameElectionDifferentOwner.Elect(time.Second * 10)
	// this should not return error
	if err != nil {
		t.Fatalf("unexpected error while performing other election:%v", err)
	}

	if ok {
		t.Fatalf("expected re-election to fail since it is held by another")
	}
	t.Logf("waiting for leader election to expire..")
	time.Sleep(time.Second * 6)
	// now we should be able to rerun election with a different owner
	ok, err = sameElectionDifferentOwner.Elect(time.Second * 5)
	if err != nil {
		t.Fatalf("unexpected error while trying to take over election:%v", err)
	}

	if !ok {
		t.Fatalf("expected taking over election to be successful")
	}
}
