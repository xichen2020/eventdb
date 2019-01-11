package index

import "github.com/pilosa/pilosa/roaring"

type bitmapBasedDocIDPositionIterator struct {
	bm        *roaring.Bitmap
	maskingIt DocIDSetIterator

	done         bool
	currDocID    int32
	currPosition int
}

// nolint: deadcode
func newBitmapBasedDocIDPositionIterator(
	bm *roaring.Bitmap,
	maskingIt DocIDSetIterator,
) *bitmapBasedDocIDPositionIterator {
	return &bitmapBasedDocIDPositionIterator{
		bm:           bm,
		maskingIt:    maskingIt,
		currDocID:    -1,
		currPosition: -1,
	}
}

func (it *bitmapBasedDocIDPositionIterator) Next() bool {
	if it.done {
		return false
	}
	if !it.maskingIt.Next() {
		it.done = true
		return false
	}
	currDocID := it.maskingIt.DocID()
	if !it.bm.Contains(uint64(currDocID)) {
		return it.Next()
	}
	// Find the number of bits set between [prevDocID+1, it.currDocID+1).
	prevDocID := it.currDocID
	it.currDocID = currDocID
	numBitsSet := it.bm.CountRange(uint64(prevDocID+1), uint64(it.currDocID+1))
	it.currPosition += int(numBitsSet)
	return true
}

func (it *bitmapBasedDocIDPositionIterator) DocID() int32 { return it.currDocID }

func (it *bitmapBasedDocIDPositionIterator) Position() int { return it.currPosition }

func (it *bitmapBasedDocIDPositionIterator) Close() {
	it.bm = nil
	it.maskingIt.Close()
	it.maskingIt = nil
}
