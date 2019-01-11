package iterator

// ForwardDoubleIterator performs forward iteration over doubles.
type ForwardDoubleIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() float64
}

// SeekableDoubleIterator is a double iterator that seek to positions.
// TODO(xichen): SeekableDoubleIterator implementations should implement this
// interface where possible to speed things up.
type SeekableDoubleIterator interface {
	ForwardDoubleIterator

	// SeekForward moves the iterator forward n positions.
	SeekForward(n int) error
}
