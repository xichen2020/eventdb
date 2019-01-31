package field

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"
)

// MultiFieldUnionIterator is an iterator that iterates over multiple fields,
// which are unioned on their doc IDs. As a result, a document needs to
// contain any of the fields associated with the iterator in order for it to
// be included in the output of the multi-field iterator.
type MultiFieldUnionIterator struct {
	iters     []BaseFieldIterator          // Used to retrieve the underlying field values
	unionIter *index.InAnyDocIDSetIterator // Used to move the base iterators in lock step

	currVals []field.ValueUnion
	hasVals  []bool
}

// NewMultiFieldUnionIterator creates a new multi-field union iterator.
func NewMultiFieldUnionIterator(iters []BaseFieldIterator) *MultiFieldUnionIterator {
	docIDIters := make([]index.DocIDSetIterator, len(iters))
	for i := 0; i < len(iters); i++ {
		docIDIters[i] = iters[i]
	}
	return &MultiFieldUnionIterator{
		iters:     iters,
		unionIter: index.NewInAnyDocIDSetIterator(docIDIters...),
		currVals:  make([]field.ValueUnion, len(iters)),
		hasVals:   make([]bool, len(iters)),
	}
}

// Next returns true if there are more items to be iterated over.
func (it *MultiFieldUnionIterator) Next() bool { return it.unionIter.Next() }

// DocID returns the current doc ID, which remains valid until the next iteration.
func (it *MultiFieldUnionIterator) DocID() int32 { return it.unionIter.DocID() }

// Values returns the current list of field values, which remains valid until the
// next iteration. If the caller needs to retain a valid refence to the value array
// after `Next` is called again, the caller needs to make a copy of the value array.
// Caller should check the boolean array to see whether the corresponding slot in the
// value array is valid.
func (it *MultiFieldUnionIterator) Values() ([]field.ValueUnion, []bool) {
	for i := 0; i < len(it.iters); i++ {
		if it.unionIter.IsDoneAt(i) || it.unionIter.DocID() != it.iters[i].DocID() {
			it.hasVals[i] = false
		} else {
			it.hasVals[i] = true
			it.currVals[i] = it.iters[i].ValueUnion()
		}
	}
	return it.currVals, it.hasVals
}

// Err returns any errors encountered.
func (it *MultiFieldUnionIterator) Err() error { return it.unionIter.Err() }

// Close closes the iterator.
func (it *MultiFieldUnionIterator) Close() {
	it.unionIter.Close() // This closes the base iters as well
	it.unionIter = nil
	it.iters = nil
	it.currVals = nil
	it.hasVals = nil
}
