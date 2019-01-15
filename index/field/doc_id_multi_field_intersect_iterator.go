package field

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"
)

// DocIDMultiFieldIntersectIterator is an iterator that iterates over multiple
// fields for values associated with documents in both the given doc ID set
// iterator and the multi field iterator.
type DocIDMultiFieldIntersectIterator struct {
	docIt        index.DocIDSetIterator
	multiFieldIt *MultiFieldIntersectIterator

	done       bool
	err        error
	currDocIDs [2]int32 // 0 for docIt, 1 for multiFieldIt
	currVals   []field.ValueUnion
}

// NewDocIDMultiFieldIntersectIterator creates a new DocIDMultiFieldIntersectIterator.
func NewDocIDMultiFieldIntersectIterator(
	docIt index.DocIDSetIterator,
	multiFieldIt *MultiFieldIntersectIterator,
) *DocIDMultiFieldIntersectIterator {
	return &DocIDMultiFieldIntersectIterator{
		docIt:        docIt,
		multiFieldIt: multiFieldIt,
	}
}

// Next returns true if there are more items to be iterated over.
func (it *DocIDMultiFieldIntersectIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	if !it.advanceDocIt() || !it.advanceMultiFieldIt() {
		return false
	}

	for {
		if it.currDocIDs[0] == it.currDocIDs[1] {
			it.currVals = it.multiFieldIt.Values()
			return true
		}
		if it.currDocIDs[0] < it.currDocIDs[1] {
			if !it.advanceDocIt() {
				return false
			}
		} else {
			if !it.advanceMultiFieldIt() {
				return false
			}
		}
	}
}

// DocID returns the current doc ID.
func (it *DocIDMultiFieldIntersectIterator) DocID() int32 { return it.currDocIDs[0] }

// Values returns the current list of field values, which remains valid until the
// next iteration. If the caller needs to retain a valid refence to the value array
// after `Next` is called again, the caller needs to make a copy of the value array.
func (it *DocIDMultiFieldIntersectIterator) Values() []field.ValueUnion {
	return it.currVals
}

// Err returns errors if any.
func (it *DocIDMultiFieldIntersectIterator) Err() error { return it.err }

// Close closes the iterator.
func (it *DocIDMultiFieldIntersectIterator) Close() {
	it.docIt.Close()
	it.docIt = nil
	it.multiFieldIt.Close()
	it.multiFieldIt = nil
	it.err = nil
}

func (it *DocIDMultiFieldIntersectIterator) advanceDocIt() bool {
	if !it.docIt.Next() {
		it.done = true
		// TODO(xichen): Add docIt.Err() here.
		return false
	}
	it.currDocIDs[0] = it.docIt.DocID()
	return true
}

func (it *DocIDMultiFieldIntersectIterator) advanceMultiFieldIt() bool {
	if !it.multiFieldIt.Next() {
		it.done = true
		it.err = it.multiFieldIt.Err()
		return false
	}
	it.currDocIDs[1] = it.multiFieldIt.DocID()
	return true
}
