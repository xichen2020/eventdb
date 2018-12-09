package encoding

import (
	"encoding/binary"
)

// ByteOrder for data serialization.
// NB(bodu): data serialization logic is dependent on endianness being little endian.
// Changing this to big endian will break int encoding/decoding.
var (
	endianness = binary.LittleEndian
)

// For allocating a buffer large enough to hold uint64 values.
const (
	uint64SizeBytes = 8
)
