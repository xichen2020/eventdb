package bytes

import "math"

// CopyDataMode determines whether to copy data.
type CopyDataMode int

// A list of supported data copy mode.
const (
	DontCopyData CopyDataMode = iota
	CopyData
)

// EnsureBufferSize returns a buffer with at least the specified target size.
// If the specified buffer has enough size, it is returned as is. Otherwise,
// a new buffer is allocated with at least the specified target size.
func EnsureBufferSize(
	buf []byte,
	targetSize int,
	copyDataMode CopyDataMode,
) []byte {
	bufSize := len(buf)
	if bufSize >= targetSize {
		return buf
	}
	newSize := int(math.Max(float64(targetSize), float64(bufSize*2)))
	newBuf := make([]byte, newSize)
	if copyDataMode == CopyData {
		copy(newBuf, buf)
	}
	return newBuf
}
