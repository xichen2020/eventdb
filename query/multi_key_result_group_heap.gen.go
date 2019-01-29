// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package query

// multiKeyResultGroupHeap is a heap storing a list of values.
// The ordering of such items are determined by `lessThanFn`.
// The smallest item will be at the top of the heap.
type multiKeyResultGroupHeap struct {
	dv         []multiKeyResultGroup
	lessThanFn func(v1, v2 multiKeyResultGroup) bool
}

// newMultiKeyResultGroupHeap creates a new values heap.
func newMultiKeyResultGroupHeap(
	initCapacity int,
	lessThanFn func(v1, v2 multiKeyResultGroup) bool,
) *multiKeyResultGroupHeap {
	return &multiKeyResultGroupHeap{
		dv:         make([]multiKeyResultGroup, 0, initCapacity),
		lessThanFn: lessThanFn,
	}
}

// RawData returns the underlying backing array in no particular order.
func (h multiKeyResultGroupHeap) RawData() []multiKeyResultGroup { return h.dv }

// Min returns the "smallest" heap element according to the `lessThan` function.
func (h multiKeyResultGroupHeap) Min() multiKeyResultGroup { return h.dv[0] }

// Len returns the number of items in the heap.
func (h multiKeyResultGroupHeap) Len() int { return len(h.dv) }

// Cap returns the heap capacity before a reallocation is needed.
func (h multiKeyResultGroupHeap) Cap() int { return cap(h.dv) }

// Less returns true if item `i` is less than item `j`.
func (h multiKeyResultGroupHeap) Less(i, j int) bool {
	return h.lessThanFn(h.dv[i], h.dv[j])
}

// Swap swaps item `i` with item `j`.
func (h multiKeyResultGroupHeap) Swap(i, j int) { h.dv[i], h.dv[j] = h.dv[j], h.dv[i] }

// Reset resets the internal backing array.
func (h *multiKeyResultGroupHeap) Reset() { h.dv = h.dv[:0] }

// Push pushes a value onto the heap.
func (h *multiKeyResultGroupHeap) Push(value multiKeyResultGroup) {
	h.dv = append(h.dv, value)
	h.shiftUp(h.Len() - 1)
}

// Pop pops a value from the heap.
func (h *multiKeyResultGroupHeap) Pop() multiKeyResultGroup {
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
func (h *multiKeyResultGroupHeap) SortInPlace() []multiKeyResultGroup {
	numElems := len(h.dv)
	for len(h.dv) > 0 {
		h.Pop()
	}
	res := h.dv[:numElems]
	h.dv = nil
	h.lessThanFn = nil
	return res
}

func (h multiKeyResultGroupHeap) shiftUp(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !h.Less(i, parent) {
			break
		}
		h.dv[parent], h.dv[i] = h.dv[i], h.dv[parent]
		i = parent
	}
}

func (h multiKeyResultGroupHeap) heapify(i, n int) {
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

// topNMultiKeyResultGroup keeps track of the top n values in a value sequence for the
// order defined by the `lessThanFn`. In particular if `lessThanFn` defines
// an increasing order (returning true if `v1` < `v2`), the collection stores
// the top N largest values, and vice versa.
type topNMultiKeyResultGroup struct {
	n          int
	lessThanFn func(v1, v2 multiKeyResultGroup) bool
	h          *multiKeyResultGroupHeap
}

// newTopNMultiKeyResultGroup creates a new top n value collection.
func newTopNMultiKeyResultGroup(
	n int,
	lessThanFn func(v1, v2 multiKeyResultGroup) bool,
) *topNMultiKeyResultGroup {
	return &topNMultiKeyResultGroup{
		n:          n,
		lessThanFn: lessThanFn,
		h:          newMultiKeyResultGroupHeap(n, lessThanFn),
	}
}

// multiKeyResultGroupAddOptions provide the options for adding a value.
type multiKeyResultGroupAddOptions struct {
	CopyOnAdd bool
	CopyFn    func(v multiKeyResultGroup) multiKeyResultGroup
	CopyToFn  func(src multiKeyResultGroup, target *multiKeyResultGroup)
}

// Len returns the number of items in the collection.
func (v topNMultiKeyResultGroup) Len() int { return v.h.Len() }

// Cap returns the collection capacity.
func (v topNMultiKeyResultGroup) Cap() int { return v.h.Cap() }

// RawData returns the underlying array backing the heap in no particular order.
func (v topNMultiKeyResultGroup) RawData() []multiKeyResultGroup { return v.h.RawData() }

// Min returns the "smallest" value according to the `lessThan` function.
func (v topNMultiKeyResultGroup) Min() multiKeyResultGroup { return v.h.Min() }

// Reset resets the internal array backing the heap.
func (v *topNMultiKeyResultGroup) Reset() { v.h.Reset() }

// Add adds a value to the collection.
func (v *topNMultiKeyResultGroup) Add(val multiKeyResultGroup, opts multiKeyResultGroupAddOptions) {
	if v.h.Len() < v.n {
		if opts.CopyOnAdd {
			val = opts.CopyFn(val)
		}
		v.h.Push(val)
		return
	}
	if min := v.h.Min(); !v.lessThanFn(min, val) {
		return
	}
	popped := v.h.Pop()
	if !opts.CopyOnAdd {
		v.h.Push(val)
		return
	}
	// Reuse popped item from the heap.
	opts.CopyToFn(val, &popped)
	v.h.Push(popped)
}

// SortInPlace sorts the backing heap in place and returns the sorted data.
// NB: The value collection becomes invalid after this is called.
func (v *topNMultiKeyResultGroup) SortInPlace() []multiKeyResultGroup {
	res := v.h.SortInPlace()
	v.h = nil
	v.lessThanFn = nil
	return res
}
