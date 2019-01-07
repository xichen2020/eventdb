package digest

import (
	"errors"

	"github.com/m3db/stackadler32"
)

const (
	numChecksumBytes = 4
)

var (
	errChecksumMismatch = errors.New("checksum mismatch")
)

// NewDigest creates a new digest.
// The default 32-bit hashing algorithm is adler32.
func NewDigest() stackadler32.Digest {
	return stackadler32.NewDigest()
}

// Checksum returns the checksum for a buffer.
func Checksum(buf []byte) uint32 {
	return stackadler32.Checksum(buf)
}

// Validate validates the data in the buffer against its checksum.
// The checksum is at the end of the buffer occupying `numChecksumBytes` bytes.
func Validate(b []byte) error {
	if len(b) < numChecksumBytes {
		return errChecksumMismatch
	}
	checksumStart := len(b) - numChecksumBytes
	expectedChecksum := ToBuffer(b[checksumStart:]).ReadDigest()
	if Checksum(b[:checksumStart]) != expectedChecksum {
		return errChecksumMismatch
	}
	return nil
}
