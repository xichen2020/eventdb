package iterator

import (
	"github.com/xichen2020/eventdb/x/bytes"
)

// ForwardBytesIterator performs forward iteration over strings.
type ForwardBytesIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() bytes.Bytes
}

// SeekableBytesIterator is a string iterator that seek to positions.
// TODO(xichen): SeekableBytesIterator implementations should implement this
// interface where possible to speed things up.
type SeekableBytesIterator interface {
	ForwardBytesIterator

	// SeekForward moves the iterator forward n positions.
	SeekForward(n int) error
}
