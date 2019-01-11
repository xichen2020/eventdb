package iterator

// ForwardStringIterator performs forward iteration over strings.
type ForwardStringIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() string
}

// SeekableStringIterator is a string iterator that seek to positions.
// TODO(xichen): SeekableStringIterator implementations should implement this
// interface where possible to speed things up.
type SeekableStringIterator interface {
	ForwardStringIterator

	// SeekForward moves the iterator forward n positions.
	SeekForward(n int) error
}
