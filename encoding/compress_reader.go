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

func newCompressReader(reader io.Reader) *CompressReader {
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

// skippableCompressReader wraps both a block reader and a compress reader.
type skippableCompressReader struct {
	br *blockReader

	*CompressReader
}

func newSkippableCompressReader(br *blockReader) *skippableCompressReader {
	return &skippableCompressReader{
		br:             br,
		CompressReader: newCompressReader(br),
	}
}

// skip to the next underlying block of data.
func (scr *skippableCompressReader) skip() error {
	return scr.br.skip()
}

func (scr *skippableCompressReader) numOfEvents() int {
	return scr.br.numOfEvents()
}
