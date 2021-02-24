package revision

import (
	"testing"

	basictestutils "github.com/khenidak/london/test/utils/basic"
)

func TestRevisioner(t *testing.T) {
	c := basictestutils.MakeTestConfig(t)
	r := NewRevisioner(c.Runtime.StorageTable)

	first, err := r.Increment()
	if err != nil {
		t.Fatalf("failed to increament:%v", err)
	}

	t.Logf("current rev:%v", first)

	added := int64(0)
	current := int64(0)
	for i := 1; i < 100; i++ {
		added = added + 1
		current, err = r.Increment()
		if err != nil {
			t.Fatalf("failed to increament:%v", err)
		}
	}

	if current != (added + first) {
		t.Fatalf("expected counter to be %v got %v", (added + first), current)
	}

	latest, err := r.Current()
	if err != nil {
		t.Fatalf("failed to get current rev")
	}

	if latest != current {
		t.Fatalf("current rev:%v got:%v", current, latest)
	}
}
