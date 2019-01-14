package index

// DocIDPositionIterator is an iterator that contains a backing doc ID set iterator
// and a "masking" doc ID set iterator. The iterator outputs the position of doc IDs
// in the backing doc ID set iterator, as well as the doc IDs at the corresponding positions.
// It implements both `DocIDSetIterator` and `PositionIterator` interface.
type DocIDPositionIterator interface {
	DocIDSetIterator

	// Position is the position in the backing doc ID set.
	Position() int

	// MaskingPosition is the position in the masking doc ID set.
	MaskingPosition() int
}

type docIDPositionIterator struct {
	backingIt DocIDSetIterator
	maskingIt DocIDSetIterator

	backingDone     bool
	maskingDone     bool
	backingDocID    int32
	maskingDocID    int32
	backingPosition int
	maskingPosition int
}

// NewDocIDPositionIterator creates a new doc ID position iterator.
func NewDocIDPositionIterator(
	backingIt DocIDSetIterator,
	maskingIt DocIDSetIterator,
) DocIDPositionIterator {
	it := &docIDPositionIterator{
		backingIt:       backingIt,
		maskingIt:       maskingIt,
		backingDocID:    invalidDocID,
		maskingDocID:    invalidDocID,
		backingPosition: -1,
		maskingPosition: -1,
	}
	it.advanceMaskingIter()
	return it
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

// Position returns the current doc ID position in the backing doc ID set.
func (it *docIDPositionIterator) Position() int { return it.backingPosition }

// MaskingPosition returns the current doc ID position in the masking doc ID set.
func (it *docIDPositionIterator) MaskingPosition() int { return it.maskingPosition }

// Close closes the iterator.
func (it *docIDPositionIterator) Close() {
	it.backingIt.Close()
	it.maskingIt.Close()
}

func (it *docIDPositionIterator) advanceBackingIter() {
	if it.backingIt.Next() {
		it.backingDocID = it.backingIt.DocID()
		it.backingPosition++
	} else {
		it.backingDone = true
	}
}

func (it *docIDPositionIterator) advanceMaskingIter() {
	if it.maskingIt.Next() {
		it.maskingDocID = it.maskingIt.DocID()
		it.maskingPosition++
	} else {
		it.maskingDone = true
	}
}
