package index

// DocIDPositionIterator is an iterator that contains a backing doc ID set iterator
// and a "masking" doc ID set iterator. The iterator outputs the position of doc IDs
// in the backing doc ID set iterator, as well as the doc IDs at the corresponding positions.
// It implements both `DocIDSetIterator` and `PositionIterator` interface.
type DocIDPositionIterator interface {
	DocIDSetIterator

	Position() int
}

type docIDPositionIterator struct {
	backingIt DocIDSetIterator
	maskingIt DocIDSetIterator

	backingDone  bool
	maskingDone  bool
	backingDocID int32
	maskingDocID int32
	currPosition int
}

// NewDocIDPositionIterator creates a new doc ID position iterator.
func NewDocIDPositionIterator(
	backingIt DocIDSetIterator,
	maskingIt DocIDSetIterator,
) DocIDPositionIterator {
	return &docIDPositionIterator{
		backingIt:    backingIt,
		maskingIt:    maskingIt,
		backingDocID: invalidDocID,
		maskingDocID: invalidDocID,
		currPosition: -1,
	}
}

// Next returns true if there are more items to be iterated over.
func (it *docIDPositionIterator) Next() bool {
	if it.backingDone || it.maskingDone {
		return false
	}
	it.advanceBackingIter()
	if it.backingDone {
		return false
	}
	if it.maskingDocID == invalidDocID {
		it.advanceMaskingIter()
	}
	for {
		if it.maskingDone {
			return false
		}
		if it.backingDocID == it.maskingDocID {
			return true
		}
		if it.backingDocID < it.maskingDocID {
			return it.Next()
		}
		it.advanceMaskingIter()
	}
}

// DocID returns the current doc ID.
func (it *docIDPositionIterator) DocID() int32 { return it.backingDocID }

// Position returns the current doc ID position.
func (it *docIDPositionIterator) Position() int { return it.currPosition }

// Close closes the iterator.
func (it *docIDPositionIterator) Close() {
	it.backingIt.Close()
	it.maskingIt.Close()
}

func (it *docIDPositionIterator) advanceBackingIter() {
	if it.backingIt.Next() {
		it.backingDocID = it.backingIt.DocID()
		it.currPosition++
	} else {
		it.backingDone = true
	}
}

func (it *docIDPositionIterator) advanceMaskingIter() {
	if it.maskingIt.Next() {
		it.maskingDocID = it.maskingIt.DocID()
	} else {
		it.maskingDone = true
	}
}
