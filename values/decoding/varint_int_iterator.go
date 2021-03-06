package decoding

import (
	"encoding/binary"
	"io"

	xio "github.com/xichen2020/eventdb/x/io"
)

// varintIntIterator iterates over a stream of
// varint encoded int data.
type varintIntIterator struct {
	reader     xio.SimpleReadCloser // `reader`is used to `Close` the underlying reader and allow for resource clean up.
	byteReader io.ByteReader        // Same as `reader` but has the proper type to save interface conversions in `Next`

	closed bool
	curr   int
	err    error
}

func newVarintIntIterator(reader xio.SimpleReadCloser) *varintIntIterator {
	return &varintIntIterator{
		reader:     reader,
		byteReader: reader,
	}
}

// Next iteration.
func (it *varintIntIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	var curr int64
	curr, it.err = binary.ReadVarint(it.byteReader)
	if it.err != nil {
		return false
	}

	it.curr = int(curr)
	return true
}

// Current returns the current int.
func (it *varintIntIterator) Current() int { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *varintIntIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close closes the iterator.
func (it *varintIntIterator) Close() {
	it.closed = true
	it.err = nil
	it.reader.Close()
	it.reader = nil
	it.byteReader = nil
}
