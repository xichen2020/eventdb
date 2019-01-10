package io

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"
)

var (
	errBufferTooSmall = errors.New("buffer too small")
	errValueTooLarge  = errors.New("value too large")
)

// WriteInt writes the lowest `n` bytes of a uint64 into a buffer.
// Precondition: n <= 8 && len(buf) >= n
func WriteInt(x uint64, n int, buf []byte) {
	for i := 0; i < n; i++ {
		buf[i] = byte(x & 0xFF)
		x >>= 8
	}
}

// ReadInt reads the lowest `n` bytes from a buffer into an uint64.
// Precondition: n <= 8 && len(buf) >= n
func ReadInt(n int, buf []byte) uint64 {
	var x uint64
	for i := n - 1; i >= 0; i-- {
		x <<= 8
		x |= uint64(buf[i])
	}
	return x
}

// WriteVarint writes an integer into an io.Writer.
func WriteVarint(writer io.Writer, v int64) error {
	var buf [binary.MaxVarintLen32]byte
	size := binary.PutVarint(buf[:], v)
	_, err := writer.Write(buf[:size])
	return err
}

// ReadVarint reads an integer from a byte slice.
func ReadVarint(data []byte) (n int64, bytesRead int, err error) {
	n, bytesRead = binary.Varint(data)
	if bytesRead > 0 {
		return n, bytesRead, nil
	}
	if bytesRead == 0 {
		return 0, 0, errBufferTooSmall
	}
	return 0, 0, errValueTooLarge
}

// VarintBytes returns the number of byts a varint occupies.
func VarintBytes(v int64) int {
	// Convert to unsigned integer first because that is how writes/reads
	// are handled in the binary package.
	uv := uint64(v) << 1
	if v < 0 {
		uv = ^uv
	}
	numBits := bits.Len64(uv)
	if numBits == 0 {
		// Return 1 byte if the value is 0.
		return 1
	}
	// Varints are base 128 encoded so we write out 7 bits
	// per byte of data. The MSB is reserved for determining continuation.
	return int(math.Ceil(float64(numBits) / 7.0))
}
