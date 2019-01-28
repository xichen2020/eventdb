package heap

import (
	"github.com/mauricelam/genny/generic"
)

// GenericValue is a value type.
type GenericValue generic.Type

// ValueHeap is a heap storing a list of values.
// The ordering of such items are determined by `lessThanFn`.
// The smallest item will be at the top of the heap.
type ValueHeap struct {
	dv         []GenericValue
	lessThanFn func(v1, v2 GenericValue) bool
}

// NewHeap creates a new values heap.
func NewHeap(
	initCapacity int,
	lessThanFn func(v1, v2 GenericValue) bool,
) *ValueHeap {
	return &ValueHeap{
		dv:         make([]GenericValue, 0, initCapacity),
		lessThanFn: lessThanFn,
	}
}

// RawData returns the underlying backing array in no particular order.
func (h ValueHeap) RawData() []GenericValue { return h.dv }

// Min returns the "smallest" heap element according to the `lessThan` function.
func (h ValueHeap) Min() GenericValue { return h.dv[0] }

// Len returns the number of items in the heap.
func (h ValueHeap) Len() int { return len(h.dv) }

// Cap returns the heap capacity before a reallocation is needed.
func (h ValueHeap) Cap() int { return cap(h.dv) }

// Less returns true if item `i` is less than item `j`.
func (h ValueHeap) Less(i, j int) bool {
	return h.lessThanFn(h.dv[i], h.dv[j])
}

// Swap swaps item `i` with item `j`.
func (h ValueHeap) Swap(i, j int) { h.dv[i], h.dv[j] = h.dv[j], h.dv[i] }

// Reset resets the internal backing array.
func (h *ValueHeap) Reset() { h.dv = h.dv[:0] }

// Push pushes a value onto the heap.
func (h *ValueHeap) Push(value GenericValue) {
	h.dv = append(h.dv, value)
	h.shiftUp(h.Len() - 1)
}

// Pop pops a value from the heap.
func (h *ValueHeap) Pop() GenericValue {
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
func (h *ValueHeap) SortInPlace() []GenericValue {
	numElems := len(h.dv)
	for len(h.dv) > 0 {
		h.Pop()
	}
	res := h.dv[:numElems]
	h.dv = nil
	h.lessThanFn = nil
	return res
}

func (h ValueHeap) shiftUp(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !h.Less(i, parent) {
			break
		}
		h.dv[parent], h.dv[i] = h.dv[i], h.dv[parent]
		i = parent
	}
}

func (h ValueHeap) heapify(i, n int) {
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
