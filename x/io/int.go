package io

// WriteInt encodes an uint64 into n bytes in a buffer.
// Precondition: n <= 8 && len(buf) >= n
func WriteInt(x uint64, n int, buf []byte) {
	for i := 0; i < n; i++ {
		buf[i] = byte(x & 0xFF)
		x >>= 8
	}
}

// ReadInt reads n bytes from a buffer into an uint64.
// Precondition: n <= 8 && len(buf) >= n
func ReadInt(n int, buf []byte) uint64 {
	var x uint64
	for i := n - 1; i >= 0; i-- {
		x <<= 8
		x |= uint64(buf[i])
	}
	return x
}
