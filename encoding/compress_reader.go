package encoding

import (
	"errors"
	"io"

	"github.com/valyala/gozstd"
)

// Errors.
var (
	ErrCompressReaderDoesNotImplementSkip = errors.New("compress reader does not implement Skip")
)

// CompressReader embeds gozstd's Reader
// and implements the `io.ByteReader` iface.
type CompressReader struct {
	buf []byte

	*gozstd.Reader
}

// NewCompressReader creates a new `CompressReader` instance.
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

// Skip is not implemented for compress reader.
func (cr *CompressReader) Skip() error {
	return ErrCompressReaderDoesNotImplementSkip
}

// SkippableCompressReader embeds CompressReader and implements a Skip function to skip underlying
// blocks of data.
type SkippableCompressReader struct {
	br *blockReader

	*CompressReader
}

func newSkippableCompressReader(
	br *blockReader,
) *SkippableCompressReader {
	return &SkippableCompressReader{
		br:             br,
		CompressReader: NewCompressReader(br),
	}
}

// Skip to the next underlying block of data.
func (scr *SkippableCompressReader) Skip() error {
	return scr.br.skip()
}

func (scr *SkippableCompressReader) numOfEvents() int {
	return scr.br.numOfEvents()
}
