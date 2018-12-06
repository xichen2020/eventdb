package encoding

import (
	"encoding/binary"

	"github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/unsafe"
)

// RawSizeStringIterator iterates over a stream of
// raw size encoded string data.
type RawSizeStringIterator struct {
	reader Reader
	extBuf *[]byte
	curr   string
	err    error
	closed bool
}

// NewRawSizeStringIterator returns a new raw size string iterator.
func NewRawSizeStringIterator(
	reader Reader,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
) *RawSizeStringIterator {
	return &RawSizeStringIterator{
		reader: reader,
		extBuf: extBuf,
	}
}

// Next iteration.
func (d *RawSizeStringIterator) Next() bool {
	if d.closed || d.err != nil {
		return false
	}

	var rawSizeBytes int64
	rawSizeBytes, d.err = binary.ReadVarint(d.reader)
	if d.err != nil {
		return false
	}

	*d.extBuf = bytes.EnsureBufferSize(*d.extBuf, int(rawSizeBytes), bytes.DontCopyData)

	_, d.err = d.reader.Read((*d.extBuf)[:rawSizeBytes])
	if d.err != nil {
		return false
	}

	d.curr = unsafe.ToString((*d.extBuf)[:rawSizeBytes])
	return true
}

// Current returns the current string.
// NB(bodu): Caller must copy the current string to have a valid reference btwn `Next()` calls.
func (d *RawSizeStringIterator) Current() string { return d.curr }

// Err returns any error recorded while iterating.
func (d *RawSizeStringIterator) Err() error { return d.err }

// Close the iterator.
func (d *RawSizeStringIterator) Close() error {
	d.closed = true
	d.extBuf = nil
	return nil
}
