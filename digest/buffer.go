package digest

import (
	"encoding/binary"
	"os"
)

const (
	// DigestLenBytes is the length of generated digests in bytes.
	DigestLenBytes = 4
)

var (
	// Endianness is little endian
	endianness = binary.LittleEndian
)

// Buffer is a byte slice that facilitates digest reading and writing.
type Buffer []byte

// NewBuffer creates a new digest buffer.
func NewBuffer() Buffer {
	return make([]byte, DigestLenBytes)
}

// WriteDigest writes a digest to the buffer.
func (b Buffer) WriteDigest(digest uint32) {
	endianness.PutUint32(b, digest)
}

// WriteDigestToFile writes a digest to the file.
func (b Buffer) WriteDigestToFile(fd *os.File, digest uint32) error {
	b.WriteDigest(digest)
	_, err := fd.Write(b)
	return err
}

// ReadDigest reads the digest from the buffer.
func (b Buffer) ReadDigest() uint32 {
	return endianness.Uint32(b)
}

// ReadDigestFromFile reads the digest from the file.
func (b Buffer) ReadDigestFromFile(fd *os.File) (uint32, error) {
	_, err := fd.Read(b)
	if err != nil {
		return 0, err
	}
	return b.ReadDigest(), nil
}

// ToBuffer converts a byte slice to a digest buffer.
func ToBuffer(buf []byte) Buffer {
	return Buffer(buf[:DigestLenBytes])
}
