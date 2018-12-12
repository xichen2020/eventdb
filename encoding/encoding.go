package encoding

import "encoding/binary"

// For allocating a buffer large enough to hold uint64 values.
const (
	uint64SizeBytes = 8
)

var endianness = binary.LittleEndian
