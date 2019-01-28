package query

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
)

// RawResult is a single raw result returned from a raw query.
// TODO(xichen): Implement `MarshalJSON` to only marshal the `Data` field without the `data tag.
type RawResult struct {
	Data string

	// Fields for joining and sorting purposes. These fields are empty for unsorted raw results.
	DocID         int32
	OrderByValues []field.ValueUnion

	// This is the index of the raw result when the raw results are ordered as dictated by the
	// query (e.g., if there are two raw results sorted by time in descending order, then the
	// first raw result has an `OrderIdx` of 0, and the second one has an `OrderIdx` of 1).
	OrderIdx int
	HasData  bool
}

// RawResultsByDocIDAsc sorts a list of raw results by their doc IDs in ascending order.
type RawResultsByDocIDAsc []RawResult

func (a RawResultsByDocIDAsc) Len() int           { return len(a) }
func (a RawResultsByDocIDAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a RawResultsByDocIDAsc) Less(i, j int) bool { return a[i].DocID < a[j].DocID }

// RawResultsByOrderIdxAsc sorts a list of doc ID values by their order indices in ascending order.
// NB(xichen): If an item does not have `HasData` set, it'll be at the end of the array after sorting.
// The ordering between two items neither of which has `HasData` set is non deterministic.
type RawResultsByOrderIdxAsc []RawResult

func (a RawResultsByOrderIdxAsc) Len() int      { return len(a) }
func (a RawResultsByOrderIdxAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a RawResultsByOrderIdxAsc) Less(i, j int) bool {
	if !a[i].HasData {
		return false
	}
	if !a[j].HasData {
		return true
	}
	return a[i].OrderIdx < a[j].OrderIdx
}

// RawResultIterator is a raw result iterator.
type RawResultIterator struct {
	resultsByDocIDAsc []RawResult

	currIdx int
}

// NewRawResultIterator creates a new raw result iterator.
func NewRawResultIterator(resultsByDocIDAsc []RawResult) *RawResultIterator {
	return &RawResultIterator{
		resultsByDocIDAsc: resultsByDocIDAsc,
		currIdx:           -1,
	}
}

// Next returns true if there are more results to be iterated over.
func (it *RawResultIterator) Next() bool {
	if it.currIdx >= len(it.resultsByDocIDAsc) {
		return false
	}
	it.currIdx++
	return it.currIdx < len(it.resultsByDocIDAsc)
}

// DocID returns the doc ID of the current raw result.
func (it *RawResultIterator) DocID() int32 { return it.resultsByDocIDAsc[it.currIdx].DocID }

// Values returns the collection of values to order the current raw results by.
func (it *RawResultIterator) Values() []field.ValueUnion {
	return it.resultsByDocIDAsc[it.currIdx].OrderByValues
}

// Err returns errors if any.
func (it *RawResultIterator) Err() error { return nil }

// Close closes the iterator.
func (it *RawResultIterator) Close() { it.resultsByDocIDAsc = nil }

// RawResultLessThanFn compares two raw results.
type RawResultLessThanFn func(v1, v2 RawResult) bool

// RawResults is a collection of raw results.
type RawResults struct {
	OrderBy                 []OrderBy
	Limit                   int
	ValuesLessThanFn        field.ValuesLessThanFn
	ResultLessThanFn        RawResultLessThanFn
	ResultReverseLessThanFn RawResultLessThanFn

	// Field types for ensuring single-type fields.
	// These are derived from the first result group processed during query execution.
	OrderByFieldTypes field.ValueTypeArray

	Data  []RawResult `json:"data"`
	cache []RawResult
}

// Len returns the number of raw results.
func (r *RawResults) Len() int { return len(r.Data) }

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
func (r *RawResults) MinOrderByValues() []field.ValueUnion {
	if r.Len() == 0 {
		return nil
	}
	return r.Data[0].OrderByValues
}

// MaxOrderByValues returns the orderBy field values for the largest result in
// the result collection.
func (r *RawResults) MaxOrderByValues() []field.ValueUnion {
	if r.Len() == 0 {
		return nil
	}
	return r.Data[r.Len()-1].OrderByValues
}

// FieldValuesLessThanFn returns the function to compare two set of field values.
func (r *RawResults) FieldValuesLessThanFn() field.ValuesLessThanFn {
	return r.ValuesLessThanFn
}

// Add adds a raw result to the collection.
// For unordered raw results:
// - If the results have not reached limit yet, the incoming result is appended at the end.
// - Otherwise, the incoming result is dropped.
// For ordered raw results:
// - If the results have not reached limit yet, the incoming result is added in order.
// - Otherwise, the incoming result is inserted and the last result is dropped.
func (r *RawResults) Add(rr RawResult) {
	if !r.IsOrdered() {
		if r.LimitReached() {
			return
		}
		if r.Data == nil {
			r.Data = make([]RawResult, 0, r.Limit)
		}
		r.Data = append(r.Data, rr)
		return
	}

	// TODO(xichen): We currently don't use `Add` for ordered raw results so punt on the
	// implementation for now. When we do, a templatized linked list might be a better choice
	// for the result collection.
	panic("not implemented")
}

// AddBatch adds a batch of raw results to the collection.
// For unordered raw results, the incoming batch is unsorted:
// - If the results have not reached limit yet, the incoming results are appended at the end
//   until the limit is reached, after which the incoming results are dropped.
// - Otherwise, the incoming results are dropped.
// For ordered raw results, the incoming batch is sorted:
// - If the results have not reached limit yet, the incoming results are added in order
//   until the limit is reached, after which the incoming results are inserted and
//   the results beyond limit are dropped.
func (r *RawResults) AddBatch(rr []RawResult) {
	// TODO(xichen): We currently don't use `AddBatch` for unordered raw results so punt on the
	// implementation for now. When we do, a templatized linked list might be a better choice
	// for the result collection.
	if !r.IsOrdered() {
		panic("not implemented")
	}
	if len(rr) == 0 {
		return
	}
	if r.Data == nil {
		r.Data = rr
		if len(r.Data) > r.Limit {
			r.Data = r.Data[:r.Limit]
		}
		return
	}

	// Ensure the cache has enough space to hold the results.
	newSize := len(r.Data) + len(rr)
	if newSize > r.Limit {
		newSize = r.Limit
	}
	if cap(r.cache) < newSize {
		r.cache = make([]RawResult, 0, r.Limit) // Potentially over-allocate a little
	}
	r.cache = r.cache[:newSize]

	// Merge results in order.
	var (
		existingIdx   = 0
		newIdx        = 0
		newResultsIdx = 0
	)
	for existingIdx < len(r.Data) && newIdx < len(rr) && newResultsIdx < newSize {
		if r.ResultLessThanFn(r.Data[existingIdx], rr[newIdx]) {
			r.cache[newResultsIdx] = r.Data[existingIdx]
			existingIdx++
		} else {
			r.cache[newResultsIdx] = rr[newIdx]
			newIdx++
		}
		newResultsIdx++
	}
	for existingIdx < len(r.Data) && newResultsIdx < newSize {
		r.cache[newResultsIdx] = r.Data[existingIdx]
		newResultsIdx++
		existingIdx++
	}
	for newIdx < len(rr) && newResultsIdx < newSize {
		r.cache[newResultsIdx] = rr[newIdx]
		newResultsIdx++
		newIdx++
	}

	// Swap data array and cache array to prepare for next add.
	r.Data, r.cache = r.cache, r.Data
}

// MergeInPlace merges the other raw results into the current raw results in place.
// Precondition: The current raw results and the other raw results are generated from the same query.
func (r *RawResults) MergeInPlace(other *RawResults) error {
	if other == nil {
		return nil
	}
	if !r.OrderByFieldTypes.Equal(other.OrderByFieldTypes) {
		return fmt.Errorf("merging two raw rsults with different order by field types %v and %v", r.OrderByFieldTypes, other.OrderByFieldTypes)
	}
	r.AddBatch(other.Data)
	other.Clear()
	return nil
}

// Clear clears the results.
func (r *RawResults) Clear() {
	r.OrderBy = nil
	r.ValuesLessThanFn = nil
	r.ResultLessThanFn = nil
	r.ResultReverseLessThanFn = nil
	r.OrderByFieldTypes = nil
	r.Data = nil
	r.cache = nil
}
