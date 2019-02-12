package decoding

import (
	"errors"
	"io"

	"github.com/valyala/gozstd"
)

var (
	errCompressedReaderClosed = errors.New("compressedReader is closed")
)

// compressedReader reads compressed data.
type compressedReader struct {
	*gozstd.Reader

	// Create a byte buffer of a single byte so
	// we can read in a single byte at a time to
	// impl the `io.ByteReader` iface.
	buf [1]byte

	closed bool
}

// newCompressedReader returns a new CompressReader instance.
func newCompressedReader(reader io.Reader) *compressedReader {
	return &compressedReader{
		Reader: gozstd.NewReader(reader),
	}
}

// ReadByte read a byte.
func (cr *compressedReader) ReadByte() (byte, error) {
	if cr.closed {
		return 0, errCompressedReaderClosed
	}
	if _, err := cr.Read(cr.buf[:]); err != nil {
		return 0, err
	}
	return cr.buf[0], nil
}

// Close releases the underlying resources of the compressed reader.
func (cr *compressedReader) Close() {
	if cr.closed {
		return
	}

	// Release does not return an error.
	cr.Release()
	cr.closed = true
}
