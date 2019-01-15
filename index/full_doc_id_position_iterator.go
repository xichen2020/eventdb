package index

// fullDocIDPositionIterator is a (doc ID, position) iterator for a full
// doc ID set representing [0, numTotalDocs) masked by the given doc ID
// set iterator `maskingIt`.
type fullDocIDPositionIterator struct {
	numTotalDocs int32
	maskingIt    DocIDSetIterator

	done            bool
	currDocID       int32
	backingPosition int
	maskingPosition int
}

func newFullDocIDPositionIterator(
	numTotalDocs int32,
	maskingIt DocIDSetIterator,
) *fullDocIDPositionIterator {
	return &fullDocIDPositionIterator{
		numTotalDocs:    numTotalDocs,
		maskingIt:       maskingIt,
		currDocID:       invalidDocID,
		backingPosition: -1,
		maskingPosition: -1,
	}
}

func (it *fullDocIDPositionIterator) Next() bool {
	if it.done {
		return false
	}
	if !it.maskingIt.Next() {
		it.done = true
		return false
	}
	it.maskingPosition++
	it.currDocID = it.maskingIt.DocID()
	if it.currDocID < it.numTotalDocs {
		it.backingPosition = int(it.currDocID)
		return true
	}
	it.done = true
	return false
}

func (it *fullDocIDPositionIterator) DocID() int32 { return it.currDocID }

func (it *fullDocIDPositionIterator) Position() int { return it.backingPosition }

func (it *fullDocIDPositionIterator) MaskingPosition() int { return it.maskingPosition }

func (it *fullDocIDPositionIterator) Close() {
	it.maskingIt.Close()
	it.maskingIt = nil
}
