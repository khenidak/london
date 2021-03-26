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
			name: "prefix-partition",
			expected: fmt.Sprintf("(%s ge '%s' and %s lt '%s')",
				consts.PartitionKeyFieldName,
				"/a/b/c/",
				consts.PartitionKeyFieldName,
				"/a/b/c0",
			),
			creator: func() Filter {
				f := NewFilter()
				f.And(PartitionKeyPrefix("/a/b/c/"))
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
		// these tests map 1:1 with filter funcs
		{
			name:     "equal",
			expected: fmt.Sprintf("%s eq '%s'", "k", "v"),
			creator: func() Filter {
				f := NewFilter()
				f.And(Equal("k", "v"))
				return f
			},
		},
		{
			name:     "not-equal",
			expected: fmt.Sprintf("%s ne '%s'", "k", "v"),
			creator: func() Filter {
				f := NewFilter()
				f.And(NotEqual("k", "v"))
				return f
			},
		},
		{
			name:     "less-than",
			expected: fmt.Sprintf("%s lt '%s'", "k", "v"),
			creator: func() Filter {
				f := NewFilter()
				f.And(LessThan("k", "v"))
				return f
			},
		},
		{
			name:     "greater-than-or-equal",
			expected: fmt.Sprintf("%s ge '%s'", "k", "v"),
			creator: func() Filter {
				f := NewFilter()
				f.And(GreaterThanOrEqual("k", "v"))
				return f
			},
		},
		{
			name:     "current-keys-only",
			expected: fmt.Sprintf("%s eq '%s'", consts.RowKeyFieldName, consts.CurrentFlag),
			creator: func() Filter {
				f := NewFilter()
				f.And(CurrentKeysOnly())
				return f
			},
		},
		{
			name: "revision-any",
			expected: fmt.Sprintf("(%s eq '%s' or %s eq '%s')",
				consts.RowKeyFieldName, "99",
				consts.RevisionFieldName, "99"),
			creator: func() Filter {
				f := NewFilter()
				f.And(RevisionAny("99"))
				return f
			},
		},
		{
			name: "current-with-revision",
			expected: fmt.Sprintf("(%s and %s eq '%s')",
				CurrentKeysOnly().Generate(),
				consts.RevisionFieldName, "99"),
			creator: func() Filter {
				f := NewFilter()
				f.And(CurrentWithRevision("99"))
				return f
			},
		},
		{
			name: "include-data-rows",
			expected: fmt.Sprintf("(%s eq '%s' and %s eq '%s')",
				consts.EntityTypeFieldName, consts.EntityTypeData,
				consts.RevisionFieldName, "99"),
			creator: func() Filter {
				f := NewFilter()
				f.And(IncludeDataRows("99"))
				return f
			},
		},
		{
			name: "include-data-rows-for-any",
			expected: fmt.Sprintf("%s eq '%s'",
				consts.EntityTypeFieldName, consts.EntityTypeData),
			creator: func() Filter {
				f := NewFilter()
				f.And(IncludeDataRowsForAny())
				return f
			},
		},
		{
			name: "exclude-events",
			expected: fmt.Sprintf("%s ne '%s'",
				consts.EntityTypeFieldName, consts.EntityTypeEvent),
			creator: func() Filter {
				f := NewFilter()
				f.And(ExcludeEvents())
				return f
			},
		},
		{
			name: "include-events",
			expected: fmt.Sprintf("%s eq '%s'",
				consts.EntityTypeFieldName, consts.EntityTypeEvent),
			creator: func() Filter {
				f := NewFilter()
				f.And(IncludeEvents())
				return f
			},
		},
		{
			name: "exclude-rows",
			expected: fmt.Sprintf("%s ne '%s'",
				consts.EntityTypeFieldName, consts.EntityTypeRow),
			creator: func() Filter {
				f := NewFilter()
				f.And(ExcludeRows())
				return f
			},
		},
		{
			name: "revision-less-than",
			expected: fmt.Sprintf("%s lt '%s'",
				consts.RevisionFieldName, "99"),
			creator: func() Filter {
				f := NewFilter()
				f.And(RevisionLessThan("99"))
				return f
			},
		},
		{
			name: "exclude-current",
			expected: fmt.Sprintf("%s ne '%s'",
				consts.RowKeyFieldName, consts.CurrentFlag),
			creator: func() Filter {
				f := NewFilter()
				f.And(ExcludeCurrent())
				return f
			},
		},
		{
			name: "exclude-sys-records",
			expected: fmt.Sprintf("(%s ne '%s' and %s ne '%s')",
				consts.PartitionKeyFieldName, consts.RevisionerPartitionKey,
				consts.PartitionKeyFieldName, consts.WriteTesterPartitionKey),
			creator: func() Filter {
				f := NewFilter()
				f.And(ExcludeSysRecords())
				return f
			},
		},
		{
			name: "deleted-keys-only",
			expected: fmt.Sprintf("%s eq '%s'",
				consts.FlagsFieldName, consts.DeletedFlag),
			creator: func() Filter {
				f := NewFilter()
				f.And(DeletedKeysOnly())
				return f
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			thisFilter := testCase.creator()
			output := thisFilter.Generate()

			if output != testCase.expected {
				t.Fatalf("filter don't match expected \nEXPECTED:%s\nGOT:%s", testCase.expected, output)
			}

		})
	}
}
