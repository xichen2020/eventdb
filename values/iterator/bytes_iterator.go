package iterator

// DataType dictates whether the underlying data is mutable or immutable.
type DataType int

// NB(bodu): Mutable data will change on each iteration and needs to be copied
// if the caller wants to keep the data around after the current iteration.
const (
	DataTypeUnknown DataType = iota
	DataTypeMutable
	DataTypeImmutable
)

// ForwardBytesIterator performs forward iteration over strings.
type ForwardBytesIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() Bytes
}

// Bytes wraps a byte slice and some meta information on whether the underlying
// data is mutable between `Next()` calls.
type Bytes struct {
	Data []byte
	Type DataType
}

// SeekableBytesIterator is a string iterator that seek to positions.
// TODO(xichen): SeekableBytesIterator implementations should implement this
// interface where possible to speed things up.
type SeekableBytesIterator interface {
	ForwardBytesIterator

	// SeekForward moves the iterator forward n positions.
	SeekForward(n int) error
}
