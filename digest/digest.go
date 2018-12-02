package digest

import (
	"hash/adler32"

	"github.com/m3db/stackadler32"
)

// NewDigest creates a new digest.
// The default 32-bit hashing algorithm is adler32.
func NewDigest() stackadler32.Digest {
	return stackadler32.NewDigest()
}

// Checksum returns the checksum for a buffer.
func Checksum(buf []byte) uint32 {
	return adler32.Checksum(buf)
}
