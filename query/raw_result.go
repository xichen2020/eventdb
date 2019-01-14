package query

import (
	"github.com/xichen2020/eventdb/document/field"
)

const (
	defaultRawResultsCapacity = 4096
)

// RawResult is a single raw result returned from a raw query.
// TODO(xichen): Implement `MarshalJSON` to only marshal the `Data` field without the `data tag.
type RawResult struct {
	Data string

	// Fields for joining and sorting purposes. These fields are empty for unsorted raw results.
	DocID         int32
	OrderByValues []field.ValueUnion
	OrderIdx      int
	HasData       bool
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

// RawResultHeap is a heap storing a list of raw results.
// The ordering of such items are determined by `compareFns`.
// The smallest item will be at the top of the heap.
type RawResultHeap struct {
	dv         []RawResult
	lessThanFn RawResultLessThanFn
}

// RawResultLessThanFn compares two raw results.
type RawResultLessThanFn func(v1, v2 RawResult) bool

// NewLessThanFn creates a less than fn from a set of field value comparison functions.
func NewLessThanFn(compareFns []field.ValueCompareFn) RawResultLessThanFn {
	return func(v1, v2 RawResult) bool {
		for idx, fn := range compareFns {
			res := fn(v1.OrderByValues[idx], v2.OrderByValues[idx])
			if res < 0 {
				return false
			}
			if res > 0 {
				return true
			}
		}
		return true
	}
}

// NewRawResultHeap creates a new raw results heap.
func NewRawResultHeap(
	capacity int,
	lessThanFn RawResultLessThanFn,
) RawResultHeap {
	initCapacity := defaultRawResultsCapacity
	if capacity >= 0 {
		initCapacity = capacity
	}
	return RawResultHeap{
		dv:         make([]RawResult, 0, initCapacity),
		lessThanFn: lessThanFn,
	}
}

// Data returns the underlying array backing the heap.
func (h RawResultHeap) Data() []RawResult { return h.dv }

// Min returns the "smallest" heap element according to the `lessThan` function.
func (h RawResultHeap) Min() RawResult { return h.dv[0] }

// Len returns the number of items in the heap.
func (h RawResultHeap) Len() int { return len(h.dv) }

// Less returns true if item `i` is less than item `j`.
func (h RawResultHeap) Less(i, j int) bool {
	return h.lessThanFn(h.dv[i], h.dv[j])
}

// Swap swaps item `i` with item `j`.
func (h RawResultHeap) Swap(i, j int) { h.dv[i], h.dv[j] = h.dv[j], h.dv[i] }

// Push pushes a raw result onto the heap.
func (h *RawResultHeap) Push(value RawResult) {
	h.dv = append(h.dv, value)
	h.shiftUp(h.Len() - 1)
}

// Pop pops a raw result from the heap.
func (h *RawResultHeap) Pop() RawResult {
	var (
		n   = h.Len()
		val = h.dv[0]
	)

	h.dv[0], h.dv[n-1] = h.dv[n-1], h.dv[0]
	h.heapify(0, n-1)
	h.dv = h.dv[0 : n-1]
	return val
}

func (h RawResultHeap) shiftUp(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !h.Less(i, parent) {
			break
		}
		h.dv[parent], h.dv[i] = h.dv[i], h.dv[parent]
		i = parent
	}
}

func (h RawResultHeap) heapify(i, n int) {
	for {
		left := i*2 + 1
		right := left + 1
		smallest := i
		if left < n && h.Less(left, smallest) {
			smallest = left
		}
		if right < n && h.Less(right, smallest) {
			smallest = right
		}
		if smallest == i {
			return
		}
		h.dv[i], h.dv[smallest] = h.dv[smallest], h.dv[i]
		i = smallest
	}
}

// RawResults is a collection of raw results.
type RawResults struct {
	IsOrdered bool
	Limit     *int

	Data []RawResult `json:"data"`
}

// Add adds a list of raw results to the collection.
func (r *RawResults) Add(rs []RawResult) {
	panic("not implemented")
}

// LimitReached returns true if we have collected enough raw results.
func (r *RawResults) LimitReached() bool {
	panic("not implemented")
}
