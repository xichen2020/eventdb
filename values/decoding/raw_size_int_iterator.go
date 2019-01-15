package decoding

import (
	"encoding/binary"
	"io"

	xio "github.com/xichen2020/eventdb/x/io"
)

// rawSizeIntIterator iterates over a stream of
// raw size encoded Int data.
// TODO(xichen): Get the buffer from bytes pool.
type rawSizeIntIterator struct {
	reader xio.Reader

	closed bool
	curr   int
	err    error
}

func newRawSizeIntIterator(reader xio.Reader) *rawSizeIntIterator {
	return &rawSizeIntIterator{reader: reader}
}

// Next iteration.
func (it *rawSizeIntIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	var curr int64
	curr, it.err = binary.ReadVarint(it.reader)
	if it.err != nil {
		return false
	}

	it.curr = int(curr)
	return true
}

// Current returns the current int.
// NB(bodu): Caller must copy the current Int to have a valid reference between `Next()` calls.
func (it *rawSizeIntIterator) Current() int { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *rawSizeIntIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close closes the iterator.
func (it *rawSizeIntIterator) Close() {
	it.closed = true
	it.err = nil
	it.reader = nil
}
