package query

import (
	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
)

// GroupedResults is a collection of result groups.
type GroupedResults struct {
	// GroupBy contains a list of field paths to group results by.
	GroupBy                     [][]string
	Calculations                []Calculation
	OrderBy                     []OrderBy
	Limit                       int
	ValuesLessThanFn            field.ValuesLessThanFn
	NewCalculationResultArrayFn calculation.NewResultArrayFromValueTypesFn

	SingleKeyGroups *SingleKeyResultGroups
	// MultiKeyGroups *MultiKeyResultGroups
}

// HasSingleKey returns true if the results are grouped by a single field as the group key.
func (r *GroupedResults) HasSingleKey() bool { return len(r.GroupBy) == 1 }

// Len returns the number of grouped results.
func (r *GroupedResults) Len() int {
	if r.HasSingleKey() {
		if r.SingleKeyGroups == nil {
			return 0
		}
		return r.SingleKeyGroups.Len()
	}
	panic("not implemented")
}

// IsOrdered returns true if the grouped results are kept in order.
func (r *GroupedResults) IsOrdered() bool { return len(r.OrderBy) > 0 }

// LimitReached returns true if we have collected enough grouped results.
func (r *GroupedResults) LimitReached() bool { return r.Len() >= r.Limit }

// IsComplete returns true if the query result is complete and can be returned
// immediately without performing any further subqueries if any. This currently
// means the result should be unordered and the result collection size has reached
// the size limit. For ordered results, we should continue performing the subqueries
// if any since there may be future results that are ordered higher than the current results.
func (r *GroupedResults) IsComplete() bool { return r.LimitReached() && !r.IsOrdered() }

// MinOrderByValues returns the orderBy field values for the smallest result in
// the result collection.
func (r *GroupedResults) MinOrderByValues() []field.ValueUnion {
	if r.Len() == 0 {
		return nil
	}
	panic("not implemented")
}

// MaxOrderByValues returns the orderBy field values for the largest result in
// the result collection.
func (r *GroupedResults) MaxOrderByValues() []field.ValueUnion {
	if r.Len() == 0 {
		return nil
	}
	panic("not implemented")
}

// FieldValuesLessThanFn returns the function to compare two set of field values.
func (r *GroupedResults) FieldValuesLessThanFn() field.ValuesLessThanFn {
	return r.ValuesLessThanFn
}

// MergeInPlace merges the other grouped results into the current grouped results in place.
// Precondition: The current grouped results and the other grouped results are generated from
// the same query.
func (r *GroupedResults) MergeInPlace(other *GroupedResults) {
	panic("not implemented")
}
