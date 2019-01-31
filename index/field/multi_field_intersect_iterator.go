package field

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"
)

// MultiFieldIntersectIterator is an iterator that iterates over multiple fields,
// which are intersected on their doc IDs. As a result, a document needs to
// contain all fields associated with the iterator in order for it to
// be included in the output of the multi-field iterator.
type MultiFieldIntersectIterator struct {
	iters         []BaseFieldIterator          // Used to retrieve the underlying field values
	intersectIter *index.InAllDocIDSetIterator // Used to move the base iterators in lock step

	currVals []field.ValueUnion
}

// NewMultiFieldIntersectIterator creates a new multi-field intersecting iterator.
func NewMultiFieldIntersectIterator(iters []BaseFieldIterator) *MultiFieldIntersectIterator {
	docIDIters := make([]index.DocIDSetIterator, len(iters))
	for i := 0; i < len(iters); i++ {
		docIDIters[i] = iters[i]
	}
	return &MultiFieldIntersectIterator{
		iters:         iters,
		intersectIter: index.NewInAllDocIDSetIterator(docIDIters...),
		currVals:      make([]field.ValueUnion, len(iters)),
	}
}

// Next returns true if there are more items to be iterated over.
func (it *MultiFieldIntersectIterator) Next() bool { return it.intersectIter.Next() }

// DocID returns the current doc ID, which remains valid until the next iteration.
func (it *MultiFieldIntersectIterator) DocID() int32 { return it.intersectIter.DocID() }

// Values returns the current list of field values, which remains valid until the
// next iteration. If the caller needs to retain a valid refence to the value array
// after `Next` is called again, the caller needs to make a copy of the value array.
func (it *MultiFieldIntersectIterator) Values() []field.ValueUnion {
	for i := 0; i < len(it.iters); i++ {
		it.currVals[i] = it.iters[i].ValueUnion()
	}
	return it.currVals
}

// Err returns any errors encountered.
func (it *MultiFieldIntersectIterator) Err() error { return it.intersectIter.Err() }

// Close closes the iterator.
func (it *MultiFieldIntersectIterator) Close() {
	it.intersectIter.Close() // This closes the base iters as well
	it.intersectIter = nil
	it.iters = nil
	it.currVals = nil
}
