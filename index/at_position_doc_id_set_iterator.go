package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values/iterator"
)

var (
	errPositionIterDocIDIterCountMismatch = errors.New("doc ID iterator and the position iterator iterator count mismatch")
)

// AtPositionDocIDSetIterator outputs the doc IDs from the doc ID set iterator at the
// given positions from the position iterator.
type AtPositionDocIDSetIterator struct {
	docIt         DocIDSetIterator
	seekableDocIt SeekableDocIDSetIterator
	positionIt    iterator.PositionIterator

	done      bool
	err       error
	firstTime bool
	currDocID int32
	currPos   int
}

// NewAtPositionDocIDSetIterator creates a new at position iterator.
func NewAtPositionDocIDSetIterator(
	docIt DocIDSetIterator,
	positionIt iterator.PositionIterator,
) *AtPositionDocIDSetIterator {
	seekableDocIt, _ := docIt.(SeekableDocIDSetIterator)
	if seekableDocIt != nil {
		docIt = nil
	}
	return &AtPositionDocIDSetIterator{
		docIt:         docIt,
		seekableDocIt: seekableDocIt,
		positionIt:    positionIt,
		firstTime:     true,
	}
}

// Next returns true if there are more doc IDs to be iterated over.
func (it *AtPositionDocIDSetIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	if !it.positionIt.Next() {
		it.done = true
		it.err = it.positionIt.Err()
		return false
	}
	nextPos := it.positionIt.Position()
	distance := nextPos - it.currPos

	// We have a next position, now advance the doc ID set iterator for the first time.
	if it.firstTime {
		it.firstTime = false
		if hasNoValues :=
			(it.seekableDocIt != nil && !it.seekableDocIt.Next()) ||
				(it.docIt != nil && !it.docIt.Next()); hasNoValues {
			it.err = errPositionIterDocIDIterCountMismatch
			return false
		}
	}

	if it.seekableDocIt != nil {
		if it.err = it.seekableDocIt.SeekForward(distance); it.err != nil {
			return false
		}
		it.currDocID = it.seekableDocIt.DocID()
	} else {
		for i := 0; i < distance; i++ {
			if !it.docIt.Next() {
				it.err = errPositionIterDocIDIterCountMismatch
				return false
			}
		}
		it.currDocID = it.docIt.DocID()
	}
	it.currPos = nextPos
	return true
}

// DocID returns the current doc ID.
func (it *AtPositionDocIDSetIterator) DocID() int32 { return it.currDocID }

// Err returns any error encountered.
func (it *AtPositionDocIDSetIterator) Err() error { return it.err }

// Close closes the iterator.
func (it *AtPositionDocIDSetIterator) Close() {
	if it.docIt != nil {
		it.docIt.Close()
		it.docIt = nil
	} else {
		it.seekableDocIt.Close()
		it.seekableDocIt = nil
	}
	it.positionIt = nil
	it.err = nil
}
