package io

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	errBufferTooSmall = errors.New("buffer too small")
	errValueTooLarge  = errors.New("value too large")
)

// Used for calculating the number of bytes required to encode a varint.
const (
	one   = (1 << 7) / 2
	two   = (1 << 14) / 2
	three = (1 << 21) / 2
	four  = (1 << 28) / 2
	five  = (1 << 35) / 2
	six   = (1 << 42) / 2
	seven = (1 << 49) / 2
	eight = (1 << 56) / 2
	nine  = (1 << 63) / 2
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

// VarintBytes returns the number of bytes required to encode a varint.
func VarintBytes(v int64) int {
	if v < one || v >= -one {
		return 1
	} else if v < two || v >= -two {
		return 2
	} else if v < three || v >= -three {
		return 3
	} else if v < four || v >= -four {
		return 4
	} else if v < five || v >= -five {
		return 5
	} else if v < six || v >= -six {
		return 6
	} else if v < seven || v >= -seven {
		return 7
	} else if v < eight || v >= -eight {
		return 8
	} else if v < nine || v >= -nine {
		return 9
	}
	return binary.MaxVarintLen64
}
