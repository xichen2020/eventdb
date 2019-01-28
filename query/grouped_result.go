package query

import (
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
)

// TODO(xichen): Make these configurable.
const (
	// By default we allow up to 100K unique groups in each intermediate result.
	// Once the result has reached the max number of groups limit, further groups
	// are dropped.
	defaultMaxNumGroupsLimit = 100000

	defaultTrimSizeLimitMultiplier   = 5
	defaultTrimSizeMinNumGroups      = 5000
	defaultTrimTriggerSizeMultiplier = 4
)

// GroupedResults is a collection of result groups.
type GroupedResults struct {
	// GroupBy contains a list of field paths to group results by.
	GroupBy      [][]string
	Calculations []Calculation
	OrderBy      []OrderBy

	// Limit is the limit defined in the raw groupBy query and limits the final number
	// of results in the response to the client sending the query.
	Limit                       int
	NewCalculationResultArrayFn calculation.NewResultArrayFromValueTypesFn

	// Field types for ensuring single-type fields.
	// These are derived from the first result group processed during query execution.
	GroupByFieldTypes field.ValueTypeArray
	CalcFieldTypes    field.ValueTypeArray

	SingleKeyGroups *SingleKeyResultGroups
	MultiKeyGroups  *MultiKeyResultGroups
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

	if r.MultiKeyGroups == nil {
		return 0
	}
	return r.MultiKeyGroups.Len()
}

// IsOrdered returns true if the grouped results are kept in order.
func (r *GroupedResults) IsOrdered() bool { return len(r.OrderBy) > 0 }

// HasOrderedFilter returns true if the raw results supports filtering ordered values.
// This is used to determine whether the result should be used to fast eliminate ineligible
// segments by filtering out those whose range fall outside the current result value range.
//
// NB(xichen): We currently do not keep results in order internally because it's fairly
// expensive to update them during result merging and not useful to order those that are
// aggregations of field values (e.g,. `Count`), which is our primary groupBy use case.
// Can revisit this assumption in the future if needed.
func (r *GroupedResults) HasOrderedFilter() bool { return false }

// LimitReached returns true if we have collected enough grouped results.
func (r *GroupedResults) LimitReached() bool { return r.Len() >= r.Limit }

// IsComplete returns true if the query result is complete and can be returned
// immediately without performing any further subqueries if any. This currently
// means the result should be unordered and the result collection size has reached
// the size limit. For ordered results, we should continue performing the subqueries
// if any since there may be future results that are ordered higher than the current results.
func (r *GroupedResults) IsComplete() bool { return r.LimitReached() && !r.IsOrdered() }

// NumGroupsLimit is the limit on the maximum total number of unique groups we keep in
// each intermediate result. For unordered group by queries, this is the same as the
// result limit. For ordered groupBy queries, this limit is usually set very high so
// we can accurately keep track of all underlying groups for majority of use cases and
// achieve a good approximation for extremely high cardinality use cases while protecting
// the server from using too much resources to track all groups for extremely high cardinality
// use cases.
func (r *GroupedResults) NumGroupsLimit() int {
	if !r.IsOrdered() {
		return r.Limit
	}
	return defaultMaxNumGroupsLimit
}

// MinOrderByValues returns the orderBy field values for the smallest result in
// the result collection.
func (r *GroupedResults) MinOrderByValues() []field.ValueUnion {
	panic("not implemented")
}

// MaxOrderByValues returns the orderBy field values for the largest result in
// the result collection.
func (r *GroupedResults) MaxOrderByValues() []field.ValueUnion {
	panic("not implemented")
}

// FieldValuesLessThanFn returns the function to compare two set of field values.
func (r *GroupedResults) FieldValuesLessThanFn() field.ValuesLessThanFn {
	panic("not implemented")
}

// Clear clears the grouped results.
func (r *GroupedResults) Clear() {
	r.GroupBy = nil
	r.Calculations = nil
	r.OrderBy = nil
	r.NewCalculationResultArrayFn = nil
	r.GroupByFieldTypes = nil
	r.CalcFieldTypes = nil
	if r.SingleKeyGroups != nil {
		r.SingleKeyGroups.Clear()
		r.SingleKeyGroups = nil
	}
	if r.MultiKeyGroups != nil {
		r.MultiKeyGroups.Clear()
		r.MultiKeyGroups = nil
	}
}

// MergeInPlace merges the other grouped results into the current grouped results in place.
// The other grouped results become invalid after the merge.
// Precondition: The current grouped results and the other grouped results are generated from
// the same query.
func (r *GroupedResults) MergeInPlace(other *GroupedResults) error {
	if other == nil {
		return nil
	}
	// NB: This also compares the number of group by fields.
	if !r.GroupByFieldTypes.Equal(other.GroupByFieldTypes) {
		return fmt.Errorf("merging two grouped results with different group by field types %v and %v", r.GroupByFieldTypes, other.GroupByFieldTypes)
	}
	if !r.CalcFieldTypes.Equal(other.CalcFieldTypes) {
		return fmt.Errorf("merging two grouped rsults with different calculation field types %v and %v", r.CalcFieldTypes, other.CalcFieldTypes)
	}
	if r.HasSingleKey() {
		if other.SingleKeyGroups == nil {
			return nil
		}
		if r.SingleKeyGroups == nil {
			r.SingleKeyGroups = other.SingleKeyGroups
			other.Clear()
			return nil
		}
		r.SingleKeyGroups.MergeInPlace(other.SingleKeyGroups)
		other.Clear()
		return nil
	}

	if other.MultiKeyGroups == nil {
		return nil
	}
	if r.MultiKeyGroups == nil {
		r.MultiKeyGroups = other.MultiKeyGroups
		other.Clear()
		return nil
	}
	r.MultiKeyGroups.MergeInPlace(other.MultiKeyGroups)
	other.Clear()
	return nil
}

// TrimIfNeeded trims the grouped results when needed based on result limit specified in the query.
func (r *GroupedResults) TrimIfNeeded() {
	if !r.shouldTrim() {
		return
	}
	r.trim()
}

// Only trim the results if this is an ordered query. For unordered query, the group limit
// is the same as the result limit, and the number of groups will never exceed the group limit,
// and as such no trimming is ever required.
func (r *GroupedResults) shouldTrim() bool {
	return r.IsOrdered() && r.Len() >= r.trimTriggerThreshold()
}

// trimTriggerThreshold is the group count threshold to trigger a trimming action.
// Since trimming is an expensive operation, we only trigger a trimming action
// if the current result size is multiple times higher than the target trim size.
func (r *GroupedResults) trimTriggerThreshold() int {
	return defaultTrimTriggerSizeMultiplier * r.trimSize()
}

// trimSize is the limit on the maximum number of ordered groups we keep in each intermediate
// result after trimming. Since it is impossible to determine the global top N results by
// merging a list of individual top N results from different nodes / shards / segments without
// keeping track of the full list of groups which is extremely expensive in high cardinality
// cases, results for ordered groupBy queries will be an approximation if the total number of
// unique groups goes beyond the `trimTriggerThreshold`. Therefore, in order to reduce the
// approximation error of the top N results due to merging and trimming, the trim size is usually
// set higher than the result limit to reduce approximation error if any.
func (r *GroupedResults) trimSize() int {
	numGroups := defaultTrimSizeMinNumGroups
	if res := defaultTrimSizeLimitMultiplier * r.Limit; numGroups < res {
		numGroups = res
	}
	return numGroups
}

// trim trims the results to the target size.
func (r *GroupedResults) trim() {
	targetSize := r.trimSize()

	if r.HasSingleKey() {
		if r.SingleKeyGroups == nil {
			return
		}
		r.SingleKeyGroups.trimToTopN(targetSize)
	}

	if r.MultiKeyGroups == nil {
		return
	}
	r.MultiKeyGroups.trimToTopN(targetSize)
}
