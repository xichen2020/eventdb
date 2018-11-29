package common

import (
	"io"
)

// Reader wraps both `io.Reader` and `io.ByteReader` interfaces.
type Reader interface {
	io.Reader
	io.ByteReader
}
