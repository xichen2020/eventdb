package decoding

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xichen2020/eventdb/x/bytes"
	xio "github.com/xichen2020/eventdb/x/io"
)

// dictionaryBasedBytesIterator iterates over a
// dict encoded stream of bytes data.
type dictionaryBasedBytesIterator struct {
	reader     xio.SimpleReadCloser // `reader`is used to `Close` the underlying reader and allow for resource clean up.
	byteReader io.ByteReader        // Same as `reader` but has the proper type to save interface conversions in `Next`
	// extDict is passed externally from the bytes decoder
	// and should not be mutated during iteration.
	extDict [][]byte

	curr []byte
	err  error
}

func newDictionaryBasedBytesIterator(
	reader xio.SimpleReadCloser,
	extDict [][]byte,
) *dictionaryBasedBytesIterator {
	return &dictionaryBasedBytesIterator{
		reader:     reader,
		byteReader: reader,
		extDict:    extDict,
	}
}

// Next iteration.
func (it *dictionaryBasedBytesIterator) Next() bool {
	// Bail early if dictionary is empty, ie values is also empty.
	if it.err != nil || len(it.extDict) == 0 {
		return false
	}

	var idx int64
	idx, it.err = binary.ReadVarint(it.byteReader)
	if it.err != nil {
		return false
	}
	if int(idx) >= len(it.extDict) {
		it.err = fmt.Errorf("bytes dictionary index %d out of range %d", idx, len(it.extDict))
		return false
	}

	it.curr = it.extDict[idx]
	return true
}

// Current returns the current bytes.
func (it *dictionaryBasedBytesIterator) Current() bytes.Bytes {
	return bytes.NewImmutableBytes(it.curr)
}

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *dictionaryBasedBytesIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close the iterator.
func (it *dictionaryBasedBytesIterator) Close() {
	it.extDict = nil
	it.err = nil
	it.reader.Close()
	it.reader = nil
	it.byteReader = nil
}
