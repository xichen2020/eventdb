package decoding

import (
	"encoding/binary"
	"io"

	"github.com/xichen2020/eventdb/x/bytes"
	xio "github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/unsafe"
)

const (
	defaultInitialStringBufferCapacity = 64
)

// rawSizeStringIterator iterates over a stream of
// raw size encoded string data.
// TODO(xichen): Get the buffer from bytes pool.
type rawSizeStringIterator struct {
	reader     xio.Reader
	byteReader io.ByteReader // Same as `reader` but has the proper type to save interface conversions in `Next`

	curr string
	err  error
	buf  []byte
}

func newRawSizeStringIterator(
	reader xio.Reader,
) *rawSizeStringIterator {
	return &rawSizeStringIterator{
		reader:     reader,
		byteReader: reader,
		buf:        make([]byte, defaultInitialStringBufferCapacity),
	}
}

// Next iteration.
func (it *rawSizeStringIterator) Next() bool {
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

	it.curr = unsafe.ToString(it.buf[:rawSizeBytes])
	return true
}

// Current returns the current string.
// NB(bodu): Caller must copy the current string to have a valid reference between `Next()` calls.
func (it *rawSizeStringIterator) Current() string { return it.curr }

// Err returns any error recorded while iterating.
// NB(xichen): This ignores `io.EOF`.
func (it *rawSizeStringIterator) Err() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close closes the iterator.
func (it *rawSizeStringIterator) Close() {
	it.buf = nil
	it.err = nil
	// Close the underlying reader if it satisifies the `io.ReadCloser` iface.
	// Since `it.reader` and `it.byteReader` reference the same reader, attempt close one of them.
	rc, ok := it.reader.(io.ReadCloser)
	if ok {
		// NB(bodu): We don't need to propagate `Close` errors back up because there aren't any.
		// We have two types of string readers. A bytes reader and a compress reader. The bytes reader
		// doesn't implement the `io.Closer` iface and the compress reader has no errors when calling `Close`.
		rc.Close()
	}
	it.reader = nil
	it.byteReader = nil
}
