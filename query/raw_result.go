package query

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
)

// RawResult is a single raw result returned from a raw query.
type RawResult struct {
	Data          string       // This is the raw doc source data
	OrderByValues field.Values // For ordering purposes, empty for unsorted raw results
}

// MarshalJSON marshals the raw results as a JSON object using the data field.
func (r RawResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Data)
}

// RawResultLessThanFn compares two raw results.
type RawResultLessThanFn func(v1, v2 RawResult) bool

// RawResults is a collection of raw results.
type RawResults struct {
	OrderBy                 []OrderBy
	Limit                   int
	ValuesLessThanFn        field.ValuesLessThanFn
	ValuesReverseLessThanFn field.ValuesLessThanFn
	ResultLessThanFn        RawResultLessThanFn
	ResultReverseLessThanFn RawResultLessThanFn

	// Field types for ensuring single-type fields.
	// These are derived from the first result group processed during query execution.
	OrderByFieldTypes field.ValueTypeArray

	// An unordered list of raw results for raw query that does not have `orderBy` clauses.
	Unordered []RawResult

	// An ordered list of raw results that keep the top N raw results according to the
	// criteria defined by the `orderBy` clauses.
	Ordered               *TopNRawResults
	minOrderByValuesReady bool
	minOrderByValues      field.Values
}

// Len returns the number of raw results.
func (r *RawResults) Len() int {
	if !r.IsOrdered() {
		return len(r.Unordered)
	}
	if r.Ordered == nil {
		return 0
	}
	return r.Ordered.Len()
}

// IsEmpty returns true if the result collection is empty.
func (r *RawResults) IsEmpty() bool { return r.Len() == 0 }

// IsOrdered returns true if the raw results are kept in order.
func (r *RawResults) IsOrdered() bool { return len(r.OrderBy) > 0 }

// HasOrderedFilter returns true if the raw results supports filtering ordered values.
// This is used to determine whether the result should be used to fast eliminate ineligible
// segments by filtering out those whose range fall outside the current result value range.
func (r *RawResults) HasOrderedFilter() bool { return r.IsOrdered() }

// LimitReached returns true if we have collected enough raw results.
func (r *RawResults) LimitReached() bool { return r.Len() >= r.Limit }

// IsComplete returns true if the query result is complete and can be returned
// immediately without performing any further subqueries if any. This currently
// means the result should be unordered and the result collection size has reached
// the size limit. For ordered results, we should continue performing the subqueries
// if any since there may be future results that are ordered higher than the current results.
func (r *RawResults) IsComplete() bool { return r.LimitReached() && !r.IsOrdered() }

// MinOrderByValues returns the orderBy field values for the smallest result in
// the result collection.
// NB: Determining the min result requires a linear scan and more expensive than determining
// the max result values. However this is okay because the limit is usually not too large so
// linear scan is tolerable. Additionally from an algorithm perspective, keeping the results
// in a heap and sorting at the end is strictly better than always keeping the results sorted
// (2NlogN + N vs 3NlogN + 2N per segment), so even with the additional O(N) complexity of
// a linear scan this is still cheaper than always keeping results sorted, and the resulting
// data structure is easier to maintain.
func (r *RawResults) MinOrderByValues() field.Values {
	if r.Len() == 0 {
		return nil
	}
	if r.minOrderByValuesReady {
		return r.minOrderByValues
	}
	data := r.Ordered.RawData()
	minOrderByValues := data[0].OrderByValues
	for i := 1; i < len(data); i++ {
		if r.ValuesLessThanFn(data[i].OrderByValues, minOrderByValues) {
			minOrderByValues = data[i].OrderByValues
		}
	}
	r.minOrderByValues = minOrderByValues
	r.minOrderByValuesReady = true
	return minOrderByValues
}

// MaxOrderByValues returns the orderBy field values for the largest result in
// the result collection.
func (r *RawResults) MaxOrderByValues() field.Values {
	if r.Len() == 0 {
		return nil
	}
	// NB: The "top" element in `Ordered` is the largest element among the top N results
	// because the heap uses `ResultReverseLessThanFn` for ordering purposes.
	return r.Ordered.Top().OrderByValues
}

// FieldValuesLessThanFn returns the function to compare two set of field values.
func (r *RawResults) FieldValuesLessThanFn() field.ValuesLessThanFn {
	return r.ValuesLessThanFn
}

// Clear clears the results.
func (r *RawResults) Clear() {
	r.OrderBy = nil
	r.ValuesLessThanFn = nil
	r.ValuesReverseLessThanFn = nil
	r.ResultLessThanFn = nil
	r.OrderByFieldTypes = nil
	r.Unordered = nil
	r.Ordered = nil
	r.minOrderByValues = nil
}

// AddUnordered adds a raw result to the unordered result collection.
// - If the results have not reached limit yet, the incoming result is appended at the end.
// - Otherwise, the incoming result is dropped.
func (r *RawResults) AddUnordered(rr RawResult) {
	if r.IsOrdered() {
		panic("attempt to call AddUnordered on ordered result collection")
	}
	if r.LimitReached() {
		return
	}
	if r.Unordered == nil {
		r.Unordered = make([]RawResult, 0, r.Limit)
	}
	r.Unordered = append(r.Unordered, rr)
}

// AddOrdered adds a raw result to the ordered result collection.
// If the results have not reached limit yet, the incoming results are added in order
// until the limit is reached, after which the incoming results are inserted and
// the results beyond limit are dropped.
func (r *RawResults) AddOrdered(rr RawResult, opts RawResultAddOptions) {
	if !r.IsOrdered() {
		panic("attempt to call AddOrdered on unordered result collection")
	}
	if r.Ordered == nil {
		r.Ordered = NewTopNRawResults(r.Limit, r.ResultReverseLessThanFn)
	}
	r.Ordered.Add(rr, opts)
	r.minOrderByValuesReady = false
	r.minOrderByValues = nil
}

// MergeInPlace merges the other raw results into the current raw results in place.
// The other raw results become invalid after the merge.
// Precondition: The current raw results and the other raw results are generated from the same query.
func (r *RawResults) MergeInPlace(other *RawResults) error {
	if other == nil || other.IsEmpty() {
		return nil
	}
	if r.IsEmpty() {
		*r = *other
		other.Clear()
		return nil
	}
	if !r.IsOrdered() {
		r.mergeUnorderedInPlace(other)
		return nil
	}
	return r.mergeOrderedInPlace(other)
}

type rawResultsJSON struct {
	Raw []RawResult `json:"raw"`
}

// MarshalJSON marshals the raw results as a JSON objedct.
func (r *RawResults) MarshalJSON() ([]byte, error) {
	rj := rawResultsJSON{Raw: r.finalData()}
	return json.Marshal(rj)
}

func (r *RawResults) mergeUnorderedInPlace(other *RawResults) {
	currData, otherData := r.Unordered, other.Unordered
	if cap(currData) < cap(otherData) {
		currData, otherData = otherData, currData
	}
	data := currData
	if cap(currData) < r.Limit {
		// This should never happen as we always allocate `r.Limit` space upfront
		// but just to be extra careful.
		data = make([]RawResult, 0, r.Limit)
		data = append(data, currData...)
	}
	targetSize := len(data) + len(otherData)
	if targetSize > r.Limit {
		targetSize = r.Limit
	}
	if toAppend := targetSize - len(data); toAppend > 0 {
		data = append(data, otherData[:toAppend]...)
	}
	r.Unordered = data[:targetSize]
	other.Clear()
}

func (r *RawResults) mergeOrderedInPlace(other *RawResults) error {
	if !r.OrderByFieldTypes.Equal(other.OrderByFieldTypes) {
		return fmt.Errorf("merging two raw results with different order by field types %v and %v", r.OrderByFieldTypes, other.OrderByFieldTypes)
	}
	currTopN, otherTopN := r.Ordered, other.Ordered
	// Using capacity instead of heap size to choose which heap to insert into because in practice
	// a reallocation is probably more expensive than doing a few more comparisons due to inserting
	// into a heap with more elements.
	if currTopN.Cap() < otherTopN.Cap() {
		currTopN, otherTopN = otherTopN, currTopN
	}
	var (
		toInsert = otherTopN.RawData()
		addOpts  RawResultAddOptions
	)
	for _, rr := range toInsert {
		currTopN.Add(rr, addOpts)
	}
	r.Ordered = currTopN
	r.minOrderByValuesReady = false
	r.minOrderByValues = nil
	other.Clear()
	return nil
}

// finalData returns the final data for the accumulated results. This is called immediately
// before sending results to the caller.
func (r *RawResults) finalData() []RawResult {
	if r.Len() == 0 {
		return nil
	}
	if !r.IsOrdered() {
		return r.Unordered
	}
	return r.Ordered.SortInPlace()
}
