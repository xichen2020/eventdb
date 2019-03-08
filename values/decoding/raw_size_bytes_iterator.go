package decoding

import (
	"encoding/binary"
	"io"

	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/bytes"
	xio "github.com/xichen2020/eventdb/x/io"
)

const (
	defaultInitialBytesBufferCapacity = 64
)

// rawSizeBytesIterator iterates over a stream of
// raw size encoded bytes data.
// TODO(xichen): Get the buffer from bytes pool.
type rawSizeBytesIterator struct {
	reader     xio.SimpleReadCloser
	byteReader io.ByteReader // Same as `reader` but has the proper type to save interface conversions in `Next`

	curr []byte
	err  error
	buf  []byte
}

func newRawSizeBytesIterator(
	reader xio.SimpleReadCloser,
) *rawSizeBytesIterator {
	return &rawSizeBytesIterator{
		reader:     reader,
		byteReader: reader,
		buf:        make([]byte, defaultInitialBytesBufferCapacity),
	}
}

// Next iteration.
func (it *rawSizeBytesIterator) Next() bool {
	if it.err != nil {
		return false
	}

	var rawSizeBytes int64
	rawSizeBytes, it.err = binary.ReadVarint(it.byteReader)
	if it.err != nil {
		return false
	}

	it.buf = bytes.EnsureBufferSize(it.buf, int(rawSizeBytes), bytes.DontCopyData)
	_, it.err = it.reader.Read(it.buf[:rawSizeBytes])
	if it.err != nil {
		return false
	}

	it.curr = it.buf[:rawSizeBytes]
	return true
}

// Current returns the current bytes.
// NB(bodu): Caller must copy the current bytes to have a valid reference between `Next()` calls.
func (it *rawSizeBytesIterator) Current() iterator.Bytes {
	return iterator.Bytes{
		Data: it.curr,
		// Underlying data changes between calls.
		Type: iterator.DataTypeMutable,
	}
}

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *rawSizeBytesIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close closes the iterator.
func (it *rawSizeBytesIterator) Close() {
	it.buf = nil
	it.err = nil
	it.reader.Close()
	it.reader = nil
	it.byteReader = nil
}
