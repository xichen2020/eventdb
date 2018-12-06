package encoding

import (
	"io"

	"github.com/valyala/gozstd"
)

// CompressReader embeds gozstd's Reader
// and implements the `io.ByteReader` iface.
type CompressReader struct {
	buf []byte

	*gozstd.Reader
}

// NewCompressReader returns a new CompressReader instance.
func NewCompressReader(reader io.Reader) *CompressReader {
	return &CompressReader{
		Reader: gozstd.NewReader(reader),
		// Create a byte buffer of a single byte so
		// we can read in a single byte at a time to
		// impl the `io.ByteReader` iface.
		buf: make([]byte, 1),
	}
}

// ReadByte implements the `io.ByteReader` iface.
func (cr *CompressReader) ReadByte() (byte, error) {
	if _, err := cr.Read(cr.buf); err != nil {
		return 0, err
	}
	return cr.buf[0], nil
}
