package index

import "github.com/pilosa/pilosa/roaring"

type bitmapBasedDocIDIterator struct {
	rit *roaring.Iterator

	closed bool
	done   bool
	curr   int32
}

func newBitmapBasedDocIDIterator(rit *roaring.Iterator) *bitmapBasedDocIDIterator {
	return &bitmapBasedDocIDIterator{rit: rit}
}

func (it *bitmapBasedDocIDIterator) Next() bool {
	if it.done || it.closed {
		return false
	}
	curr, eof := it.rit.Next()
	if eof {
		it.done = true
		return false
	}
	it.curr = int32(curr)
	return true
}

func (it *bitmapBasedDocIDIterator) DocID() int32 { return it.curr }

func (it *bitmapBasedDocIDIterator) Err() error { return nil }

func (it *bitmapBasedDocIDIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.rit = nil
}
