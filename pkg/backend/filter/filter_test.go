package Filter

import (
	"fmt"

	"testing"

	"github.com/khenidak/london/pkg/backend/consts"
)

func TestFilter(t *testing.T) {
	testCases := []struct {
		name     string
		expected string
		creator  func() Filter
	}{
		{
			name:     "basic partition key",
			expected: fmt.Sprintf("%s eq '%s'", consts.PartitionKeyFieldName, "p1"),
			creator: func() Filter {
				f := NewFilter()
				f.And(Partition("p1"))
				return f
			},
		},

		{
			name:     "condition and combinedAnd",
			expected: fmt.Sprintf("%s eq '%s' and (f1 eq 'f1value' and f2 eq 'f2value')", consts.PartitionKeyFieldName, "p1"),
			creator: func() Filter {
				f := NewFilter()
				f.And(Partition("p1"))
				f.And(CombineAnd(
					Equal("f1", "f1value"),
					Equal("f2", "f2value")))
				return f
			},
		},

		{
			name:     "condition and combinedOr",
			expected: fmt.Sprintf("%s eq '%s' and (f1 eq 'f1value' or f2 eq 'f2value')", consts.PartitionKeyFieldName, "p1"),
			creator: func() Filter {
				f := NewFilter()
				f.And(Partition("p1"))
				f.And(CombineOr(
					Equal("f1", "f1value"),
					Equal("f2", "f2value")))
				return f
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			thisFilter := testCase.creator()
			output := thisFilter.Generate()

			if output != testCase.expected {
				t.Fatalf("filter don't match expected \n%s\n%s", testCase.expected, output)
			}

		})
	}
}
