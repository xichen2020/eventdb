package encoding

import (
	"io"
)

// Reader is both an io.Reader and an io.ByteReader.
type Reader interface {
	io.Reader
	io.ByteReader
}
