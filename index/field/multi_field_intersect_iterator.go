package field

import (
	"github.com/xichen2020/eventdb/document/field"
)

// MultiFieldIntersectIterator is an iterator that iterates over multiple fields,
// which are joined on their doc IDs. As a result, a document needs to
// contain all fields associated with the iterator in order for it to
// be included in the output of the multi-field iterator.
type MultiFieldIntersectIterator struct {
	iters []BaseFieldIterator

	done       bool
	err        error
	currDocIDs []int32
	currVals   []field.ValueUnion
}

// NewMultiFieldIntersectIterator creates a new multi-field intersecting iterator.
func NewMultiFieldIntersectIterator(iters []BaseFieldIterator) *MultiFieldIntersectIterator {
	return &MultiFieldIntersectIterator{
		iters:      iters,
		done:       len(iters) == 0,
		currDocIDs: make([]int32, len(iters)),
		currVals:   make([]field.ValueUnion, len(iters)),
	}
}

// Next returns true if there are more items to be iterated over.
func (it *MultiFieldIntersectIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	// Advance all iterators first.
	for i, iit := range it.iters {
		if !iit.Next() {
			it.done = true
			it.err = iit.Err()
			return false
		}
		it.currDocIDs[i] = iit.DocID()
	}

	for {
		var minIdx, maxIdx int
		for i := 1; i < len(it.currDocIDs); i++ {
			if it.currDocIDs[i] < it.currDocIDs[minIdx] {
				minIdx = i
			}
			if it.currDocIDs[i] > it.currDocIDs[maxIdx] {
				maxIdx = i
			}
		}
		// All iterators have the same doc ID.
		if it.currDocIDs[minIdx] == it.currDocIDs[maxIdx] {
			for i := 0; i < len(it.iters); i++ {
				it.currVals[i] = it.iters[i].ValueUnion()
			}
			return true
		}
		if !it.iters[minIdx].Next() {
			it.done = true
			it.err = it.iters[minIdx].Err()
			return false
		}
		it.currDocIDs[minIdx] = it.iters[minIdx].DocID()
	}
}

// DocID returns the current doc ID, which remains valid until the next iteration.
func (it *MultiFieldIntersectIterator) DocID() int32 { return it.currDocIDs[0] }

// Values returns the current list of field values, which remains valid until the
// next iteration. If the caller needs to retain a valid refence to the value array
// after `Next` is called again, the caller needs to make a copy of the value array.
func (it *MultiFieldIntersectIterator) Values() []field.ValueUnion { return it.currVals }

// Err returns errors if any.
func (it *MultiFieldIntersectIterator) Err() error { return it.err }

// Close closes the iterator.
func (it *MultiFieldIntersectIterator) Close() {
	for i := range it.iters {
		it.iters[i].Close()
		it.iters[i] = nil
	}
	it.iters = nil
	it.err = nil
	it.currVals = nil
}
