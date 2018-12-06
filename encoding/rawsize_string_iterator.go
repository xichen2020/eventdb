package encoding

import (
	"encoding/binary"

	"github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/unsafe"
)

type rawSizeStringIterator struct {
	reader Reader
	buf    []byte
	curr   string
	err    error
	closed bool
	zero   uint64
}

func newRawSizeStringIterator(reader Reader) *rawSizeStringIterator {
	return &rawSizeStringIterator{
		reader: reader,
		buf:    make([]byte, Uint64SizeBytes),
	}
}

func (d *rawSizeStringIterator) Next() bool {
	if d.closed || d.err != nil {
		return false
	}

	// Zero out buf before each iteration.
	endianness.PutUint64(d.buf, d.zero)

	var rawSizeBytes int64
	rawSizeBytes, d.err = binary.ReadVarint(d.reader)
	if d.err != nil {
		return false
	}

	d.buf = bytes.EnsureBufferSize(d.buf, int(rawSizeBytes), bytes.DontCopyData)
	_, d.err = d.reader.Read(d.buf[:rawSizeBytes])
	if d.err != nil {
		return false
	}

	d.curr = unsafe.ToString(d.buf[:rawSizeBytes])

	return true
}

// NB(bodu): Caller must copy the current string to have a valid reference btwn `Next()` calls.
func (d *rawSizeStringIterator) Current() string {
	return d.curr
}

func (d *rawSizeStringIterator) Err() error {
	return d.err
}

func (d *rawSizeStringIterator) Close() error {
	d.closed = true
	return nil
}
