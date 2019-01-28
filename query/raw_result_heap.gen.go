// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package query

// RawResultHeap is a heap storing a list of values.
// The ordering of such items are determined by `lessThanFn`.
// The smallest item will be at the top of the heap.
type RawResultHeap struct {
	dv         []RawResult
	lessThanFn func(v1, v2 RawResult) bool
}

// NewRawResultHeap creates a new values heap.
func NewRawResultHeap(
	initCapacity int,
	lessThanFn func(v1, v2 RawResult) bool,
) *RawResultHeap {
	return &RawResultHeap{
		dv:         make([]RawResult, 0, initCapacity),
		lessThanFn: lessThanFn,
	}
}

// RawData returns the underlying backing array in no particular order.
func (h RawResultHeap) RawData() []RawResult { return h.dv }

// Min returns the "smallest" heap element according to the `lessThan` function.
func (h RawResultHeap) Min() RawResult { return h.dv[0] }

// Len returns the number of items in the heap.
func (h RawResultHeap) Len() int { return len(h.dv) }

// Cap returns the heap capacity before a reallocation is needed.
func (h RawResultHeap) Cap() int { return cap(h.dv) }

// Less returns true if item `i` is less than item `j`.
func (h RawResultHeap) Less(i, j int) bool {
	return h.lessThanFn(h.dv[i], h.dv[j])
}

// Swap swaps item `i` with item `j`.
func (h RawResultHeap) Swap(i, j int) { h.dv[i], h.dv[j] = h.dv[j], h.dv[i] }

// Reset resets the internal backing array.
func (h *RawResultHeap) Reset() { h.dv = h.dv[:0] }

// Push pushes a value onto the heap.
func (h *RawResultHeap) Push(value RawResult) {
	h.dv = append(h.dv, value)
	h.shiftUp(h.Len() - 1)
}

// Pop pops a value from the heap.
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

// SortInPlace sorts the heap in place and returns the sorted data, with the smallest element
// at the end of the returned array. This is done by repeated swapping the smallest element with
// the last element of the current heap and shrinking the heap size.
// NB: The heap becomes invalid after this is called.
func (h *RawResultHeap) SortInPlace() []RawResult {
	numElems := len(h.dv)
	for len(h.dv) > 0 {
		h.Pop()
	}
	res := h.dv[:numElems]
	h.dv = nil
	h.lessThanFn = nil
	return res
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
