package decoding

import (
	"io"

	"github.com/valyala/gozstd"
)

// compressedReader reads compressed data.
type compressedReader struct {
	*gozstd.Reader

	// Create a byte buffer of a single byte so
	// we can read in a single byte at a time to
	// impl the `io.ByteReader` iface.
	buf [1]byte
}

// newCompressedReader returns a new CompressReader instance.
func newCompressedReader(reader io.Reader) *compressedReader {
	return &compressedReader{
		Reader: gozstd.NewReader(reader),
	}
}

// ReadByte read a byte.
func (cr *compressedReader) ReadByte() (byte, error) {
	if _, err := cr.Read(cr.buf[:]); err != nil {
		return 0, err
	}
	return cr.buf[0], nil
}

// Close releases the underlying resources of the compressed reader.
func (cr *compressedReader) Close() error {
	// Release does not return an error.
	cr.Release()
	return nil
}
