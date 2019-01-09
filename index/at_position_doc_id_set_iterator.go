package index

import (
	"github.com/xichen2020/eventdb/values/iterator"
)

// AtPositionDocIDSetIterator outputs the doc IDs from the doc ID set iterator at the
// given positions from the position iterator.
type AtPositionDocIDSetIterator struct {
	docIt      DocIDSetIterator
	positionIt iterator.PositionIterator

	currDocID int32
	currPos   int
	done      bool
}

// NewAtPositionDocIDSetIterator creates a new at position iterator.
func NewAtPositionDocIDSetIterator(
	docIt DocIDSetIterator,
	positionIt iterator.PositionIterator,
) *AtPositionDocIDSetIterator {
	return &AtPositionDocIDSetIterator{
		docIt:      docIt,
		positionIt: positionIt,
	}
}

// Next returns true if there are more doc IDs to be iterated over.
func (it *AtPositionDocIDSetIterator) Next() bool {
	if it.done {
		return false
	}
	if !it.positionIt.Next() {
		it.done = true
		return false
	}
	nextPos := it.positionIt.Current()
	distance := nextPos - it.currPos
	// TODO(xichen): Look into optimizations to speed this up if the doc ID set iterator
	// supports a `Seek` or `Advance` API.
	for i := 0; i < distance; i++ {
		if !it.docIt.Next() {
			panic("doc ID iterator and the position iterator iterator count mismatch")
		}
	}
	it.currDocID = it.docIt.DocID()
	it.currPos = nextPos
	return true
}

// DocID returns the current doc ID.
func (it *AtPositionDocIDSetIterator) DocID() int32 { return it.currDocID }

// Close closes the iterator.
func (it *AtPositionDocIDSetIterator) Close() {
	it.docIt.Close()
	it.docIt = nil
	it.positionIt = nil
}
