package encoding

import (
	"encoding/binary"

	"github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/unsafe"
)

// RawSizeStringIterator iterates over a stream of
// raw size encoded string data.
type RawSizeStringIterator struct {
	reader io.Reader
	extBuf *[]byte
	curr   string
	err    error
	closed bool
}

func newRawSizeStringIterator(
	reader io.Reader,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
) *RawSizeStringIterator {
	return &RawSizeStringIterator{
		reader: reader,
		extBuf: extBuf,
	}
}

// Next iteration.
func (it *RawSizeStringIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	var rawSizeBytes int64
	rawSizeBytes, it.err = binary.ReadVarint(it.reader)
	if it.err != nil {
		return false
	}

	*it.extBuf = bytes.EnsureBufferSize(*it.extBuf, int(rawSizeBytes), bytes.DontCopyData)

	_, it.err = it.reader.Read((*it.extBuf)[:rawSizeBytes])
	if it.err != nil {
		return false
	}

	it.curr = unsafe.ToString((*it.extBuf)[:rawSizeBytes])
	return true
}

// Current returns the current string.
// NB(bodu): Caller must copy the current string to have a valid reference btwn `Next()` calls.
func (it *RawSizeStringIterator) Current() string { return it.curr }

// Err returns any error recorded while iterating.
func (it *RawSizeStringIterator) Err() error { return it.err }

// Close the iterator.
func (it *RawSizeStringIterator) Close() error {
	it.closed = true
	it.extBuf = nil
	it.err = nil
	it.reader = nil
	return nil
}
