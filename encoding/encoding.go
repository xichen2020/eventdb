package encoding

import (
	"encoding/binary"
)

// ByteOrder for data serialization.
var (
	endianness = binary.LittleEndian
)

// For allocating a buffer large enough to hold uint64 values.
const (
	uint64SizeBytes = 8
)
