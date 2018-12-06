package encoding

import (
	"encoding/binary"
	"io"
)

// Endianness.
var (
	endianness = binary.LittleEndian
)

// Reader is both an io.Reader and an io.ByteReader.
type Reader interface {
	io.Reader
	io.ByteReader
}
