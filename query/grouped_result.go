package query

import (
	"errors"
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
)

// TODO(xichen): Make these configurable.
const (
	// By default we allow up to 100K unique groups in each intermediate result.
	// Once the result has reached the max number of groups limit, further groups
	// are dropped. This is chosen so group by queries should never hit this limit
	// unless the query is grouping by fields with extremely high cardinality, in
	// which case this limit protects the database from running out of memory while
	// still being able to produce a reasonable groupBy result.
	defaultMaxNumGroupsLimit = 100000

	// By default the target number of groups to trim group results to should have
	// at least 5 * limit groups. This is so that the number of groups after trimming
	// is sufficiently large compared to the query limit to reduce the approximation errors when
	// merging top N results from different segments / shards / nodes to produce the
	// final top N results.
	defaultTrimSizeLimitMultiplier = 5

	// By default the target number of groups to trim group results to should have
	// at least 5000 groups. This is so that the minimum number of groups is sufficiently
	// large compared to the query limit to reduce the approximation errors when merging
	// top N results from different segments / shards / nodes to produce the final top N results.
	defaultTrimSizeMinNumGroups = 5000

	// By default we trigger a trimming action if the total number of groups is
	// at least 4 times the target number of groups determined by the trim size.
	defaultTrimTriggerSizeMultiplier = 4
)

var (
	emptyJSONResponse = []byte("{}")

	errNilGroupedQueryResultsProto         = errors.New("nil grouped query results proto")
	errNoResultsInGroupedQueryResultsProto = errors.New("no results set in grouped results proto")
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
	CalcFieldTypes    field.OptionalTypeArray

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

// IsEmpty returns true if the result collection is empty.
func (r *GroupedResults) IsEmpty() bool { return r.Len() == 0 }

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

// Clear clears the grouped results.
func (r *GroupedResults) Clear() {
	r.GroupBy = nil
	r.Calculations = nil
	r.OrderBy = nil
	r.NewCalculationResultArrayFn = nil
	r.GroupByFieldTypes = nil
	r.CalcFieldTypes = nil
	r.SingleKeyGroups = nil
	r.MultiKeyGroups = nil
}

// MergeInPlace merges the other grouped results into the current grouped results in place.
// The other grouped results become invalid after the merge.
// Precondition: The current grouped results and the other grouped results are generated from
// the same query.
func (r *GroupedResults) MergeInPlace(other *GroupedResults) error {
	if other == nil || other.IsEmpty() {
		return nil
	}
	if r.IsEmpty() {
		*r = *other
		other.Clear()
		return nil
	}
	// NB: This also compares the number of group by fields.
	if !r.GroupByFieldTypes.Equal(other.GroupByFieldTypes) {
		return fmt.Errorf("merging two grouped results with different group by field types %v and %v", r.GroupByFieldTypes, other.GroupByFieldTypes)
	}
	if err := r.CalcFieldTypes.MergeInPlace(other.CalcFieldTypes); err != nil {
		return fmt.Errorf("error merging calculation field types %v and %v in two grouped results: %v", r.CalcFieldTypes, other.CalcFieldTypes, err)
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

// MarshalJSON marshals the grouped results as a JSON object.
func (r *GroupedResults) MarshalJSON() ([]byte, error) {
	if r.IsEmpty() {
		return emptyJSONResponse, nil
	}
	var (
		limit        = r.Limit
		topNRequired = r.IsOrdered()
	)
	if r.HasSingleKey() {
		return r.SingleKeyGroups.MarshalJSON(limit, topNRequired)
	}
	return r.MultiKeyGroups.MarshalJSON(limit, topNRequired)
}

// ToProto converts the grouped results to grouped results proto messages.
func (r *GroupedResults) ToProto() (*servicepb.GroupedQueryResults, error) {
	var (
		limit        = r.Limit
		topNRequired = r.IsOrdered()
	)

	if r.HasSingleKey() {
		singleKeyResults := r.SingleKeyGroups.ToProto(limit, topNRequired)
		return &servicepb.GroupedQueryResults{
			Results: &servicepb.GroupedQueryResults_SingleKey{
				SingleKey: singleKeyResults,
			},
		}, nil
	}

	multiKeyResults, err := r.MultiKeyGroups.ToProto(limit, topNRequired)
	if err != nil {
		return nil, err
	}
	return &servicepb.GroupedQueryResults{
		Results: &servicepb.GroupedQueryResults_MultiKey{
			MultiKey: multiKeyResults,
		},
	}, nil
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
// keeping track of the full list of groups which is expensive in extremely high cardinality
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

// GroupedQueryResults is a union that contains the results for a grouped query.
// Only one of the fields should be set. This is used to send results back to clients.
type GroupedQueryResults struct {
	SingleKey *SingleKeyGroupQueryResults `json:"singleKey"`
	MultiKey  *MultiKeyGroupQueryResults  `json:"multiKey"`
}

// NewGroupedQueryResultsFromProto creates a new grouped query results from
// grouped query results protobuf message.
func NewGroupedQueryResultsFromProto(
	pbRes *servicepb.GroupedQueryResults,
) (*GroupedQueryResults, error) {
	if pbRes == nil {
		return nil, errNilGroupedQueryResultsProto
	}

	if pbSingleKeyResults := pbRes.GetSingleKey(); pbSingleKeyResults != nil {
		singleKeyResults, err := NewSingleKeyGroupedQueryResultsFromProto(pbSingleKeyResults)
		if err != nil {
			return nil, err
		}
		return &GroupedQueryResults{
			SingleKey: singleKeyResults,
		}, nil
	}

	pbMultiKeyResults := pbRes.GetMultiKey()
	if pbMultiKeyResults == nil {
		return nil, errNoResultsInGroupedQueryResultsProto
	}
	multiKeyResults, err := NewMultiKeyGroupedQueryResultsFromProto(pbMultiKeyResults)
	if err != nil {
		return nil, err
	}
	return &GroupedQueryResults{
		MultiKey: multiKeyResults,
	}, nil
}

// SingleKeyGroupQueryResult contains the result for a single-key group.
type SingleKeyGroupQueryResult struct {
	Key    field.ValueUnion   `json:"key"`
	Values calculation.Values `json:"values"`
}

// NewSingleKeyGroupedQueryResultFromProto creates a new single-key grouped query
// result from protobuf message.
func NewSingleKeyGroupedQueryResultFromProto(
	pbRes servicepb.SingleKeyGroupQueryResult,
) (SingleKeyGroupQueryResult, error) {
	key, err := field.NewValueFromProto(pbRes.Key)
	if err != nil {
		return SingleKeyGroupQueryResult{}, err
	}
	values, err := calculation.NewValuesFromProto(pbRes.Values)
	if err != nil {
		return SingleKeyGroupQueryResult{}, err
	}
	return SingleKeyGroupQueryResult{Key: key, Values: values}, nil
}

// SingleKeyGroupQueryResults contains the results for a single-groupBy-key grouped query.
type SingleKeyGroupQueryResults struct {
	Groups []SingleKeyGroupQueryResult `json:"groups"`
}

// NewSingleKeyGroupedQueryResultsFromProto creates a new single-key grouped query
// results from protobuf message.
func NewSingleKeyGroupedQueryResultsFromProto(
	pbRes *servicepb.SingleKeyGroupQueryResults,
) (*SingleKeyGroupQueryResults, error) {
	groups := make([]SingleKeyGroupQueryResult, 0, len(pbRes.Groups))
	for _, pbGroup := range pbRes.Groups {
		group, err := NewSingleKeyGroupedQueryResultFromProto(pbGroup)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return &SingleKeyGroupQueryResults{
		Groups: groups,
	}, nil
}

// MultiKeyGroupQueryResult contains the result for a multi-key group.
type MultiKeyGroupQueryResult struct {
	Key    field.Values       `json:"key"`
	Values calculation.Values `json:"values"`
}

// MultiKeyGroupQueryResults contains the result for a multi-groupBy-key grouped query.
type MultiKeyGroupQueryResults struct {
	Groups []MultiKeyGroupQueryResult `json:"groups"`
}

// NewMultiKeyGroupedQueryResultFromProto creates a new multi-key grouped query
// result from protobuf message.
func NewMultiKeyGroupedQueryResultFromProto(
	pbRes servicepb.MultiKeyGroupQueryResult,
) (MultiKeyGroupQueryResult, error) {
	key, err := field.NewValuesFromProto(pbRes.Key)
	if err != nil {
		return MultiKeyGroupQueryResult{}, err
	}
	values, err := calculation.NewValuesFromProto(pbRes.Values)
	if err != nil {
		return MultiKeyGroupQueryResult{}, err
	}
	return MultiKeyGroupQueryResult{Key: key, Values: values}, nil
}

// NewMultiKeyGroupedQueryResultsFromProto creates a new multi-key grouped query
// results from protobuf message.
func NewMultiKeyGroupedQueryResultsFromProto(
	pbRes *servicepb.MultiKeyGroupQueryResults,
) (*MultiKeyGroupQueryResults, error) {
	groups := make([]MultiKeyGroupQueryResult, 0, len(pbRes.Groups))
	for _, pbGroup := range pbRes.Groups {
		group, err := NewMultiKeyGroupedQueryResultFromProto(pbGroup)
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return &MultiKeyGroupQueryResults{
		Groups: groups,
	}, nil
}
