package encoding

import (
	"io"

	"github.com/valyala/gozstd"
)

// CompressReader needs to create a byte buf of 1 byte
// in length to implement the `io.ByteReader` iface.
const (
	oneByteLength = 1
	firstByteIdx  = 0
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
		buf:    make([]byte, oneByteLength),
	}
}

// ReadByte implements the `io.ByteReader` iface.
func (cr *CompressReader) ReadByte() (byte, error) {
	if _, err := cr.Read(cr.buf); err != nil {
		return cr.buf[firstByteIdx], err
	}
	return cr.buf[firstByteIdx], nil
}
