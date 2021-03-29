package Filter

import (
	"fmt"

	"github.com/khenidak/london/pkg/backend/consts"
)

type Filter interface {
	And(filters ...Filter)
	Or(filters ...Filter)
	Generate() string
}

type filter struct {
	predicates string
}

func NewFilter() Filter {
	return &filter{}
}

func (f *filter) And(filters ...Filter) {
	for _, thisFilter := range filters {
		if len(f.predicates) == 0 {
			f.predicates = thisFilter.Generate()
		} else {
			f.predicates = fmt.Sprintf("%s and %s", f.predicates, thisFilter.Generate())
		}
	}
}

func (f *filter) Or(filters ...Filter) {
	for _, thisFilter := range filters {
		if len(f.predicates) == 0 {
			f.predicates = thisFilter.Generate()
		} else {
			f.predicates = fmt.Sprintf("%s or %s", f.predicates, thisFilter.Generate())
		}
	}
}

func (f *filter) Generate() string {
	return f.predicates
}

//  (filter *and* filter *and* filter ..)
func CombineAnd(filters ...Filter) Filter {
	predicates := ""
	for idx, thisFilter := range filters {
		if idx != 0 {
			predicates = fmt.Sprintf("%s and %s", predicates, thisFilter.Generate())
		} else {
			predicates = thisFilter.Generate()
		}
	}

	predicates = fmt.Sprintf("(%s)", predicates)
	return &filter{
		predicates: predicates,
	}
}

// (filter *or* filter *or* filter ..)
func CombineOr(filters ...Filter) Filter {
	predicates := ""
	for idx, thisFilter := range filters {
		if idx != 0 {
			predicates = fmt.Sprintf("%s or %s", predicates, thisFilter.Generate())
		} else {
			predicates = thisFilter.Generate()
		}
	}

	predicates = fmt.Sprintf("(%s)", predicates)
	return &filter{
		predicates: predicates,
	}
}

// x eq y
func Equal(field string, value string) Filter {
	return &filter{
		predicates: fmt.Sprintf("%s eq '%s'", field, value),
	}
}

//x ne y
func NotEqual(field string, value string) Filter {
	return &filter{
		predicates: fmt.Sprintf("%s ne '%s'", field, value),
	}
}

// x lt y
func LessThan(field string, value string) Filter {
	return &filter{
		predicates: fmt.Sprintf("%s lt '%s'", field, value),
	}
}

// x ge y
func GreaterThanOrEqual(field string, value string) Filter {
	return &filter{
		predicates: fmt.Sprintf("%s ge '%s'", field, value),
	}
}

// returns a filter expression for PartitionKey
func Partition(pKey string) Filter {
	return Equal(consts.PartitionKeyFieldName, pKey)
}

func PartitionKeyPrefix(pKey string) Filter {
	start, end := prefixEndFromKey(pKey)
	return CombineAnd(
		GreaterThanOrEqual(consts.PartitionKeyFieldName, start),
		LessThan(consts.PartitionKeyFieldName, end),
	)
}

func CurrentKeysOnly() Filter {
	return Equal(consts.RowKeyFieldName, consts.CurrentFlag)
}

func RevisionAny(revision string) Filter {
	return CombineOr(
		Equal(consts.RowKeyFieldName, revision),
		Equal(consts.RevisionFieldName, revision),
	)
}

func CurrentWithRevision(revision string) Filter {
	return CombineAnd(
		CurrentKeysOnly(),
		Equal(consts.RevisionFieldName, revision),
	)
}

func IncludeDataRows(revision string) Filter {
	return CombineAnd(
		Equal(consts.EntityTypeFieldName, consts.EntityTypeData),
		Equal(consts.RevisionFieldName, revision),
	)
}

func IncludeDataRowsForAny() Filter {
	return Equal(consts.EntityTypeFieldName, consts.EntityTypeData)
}

func ExcludeEvents() Filter {
	return NotEqual(consts.EntityTypeFieldName, consts.EntityTypeEvent)
}
func ExcludeRows() Filter {
	return NotEqual(consts.EntityTypeFieldName, consts.EntityTypeRow)
}

func IncludeEvents() Filter {
	return Equal(consts.EntityTypeFieldName, consts.EntityTypeEvent)
}

// for non current
func RevisionLessThan(revision string) Filter {
	return LessThan(consts.RevisionFieldName, revision)
}

func ExcludeCurrent() Filter {
	return NotEqual(consts.RowKeyFieldName, consts.CurrentFlag)
}

func ExcludeSysRecords() Filter {
	return CombineAnd(
		NotEqual(consts.PartitionKeyFieldName, consts.RevisionerPartitionKey),
		NotEqual(consts.PartitionKeyFieldName, consts.WriteTesterPartitionKey),
	)
}

func DeletedKeysOnly() Filter {
	return Equal(consts.FlagsFieldName, consts.DeletedFlag)
}

func LeaderElectionEntity(electionName string) Filter {
	return CombineAnd(
		Equal(consts.PartitionKeyFieldName, consts.LeaderElectPartitionName),
		Equal(consts.RowKeyFieldName, electionName),
		Equal(consts.EntityTypeFieldName, consts.EntityTypeLeaderElection),
	)
}
func prefixEndFromKey(key string) (start string, end string) {
	last := key[len(key)-1]
	last = last + 1
	end = key[:len(key)-1] + string(last)

	return key, end
}
