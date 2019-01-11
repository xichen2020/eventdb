package iterator

// ForwardBoolIterator performs forward iteration over bools.
type ForwardBoolIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() bool
}

// SeekableBoolIterator is a bool iterator that seek to positions.
// TODO(xichen): SeekableBoolIterator implementations should implement this
// interface where possible to speed things up.
type SeekableBoolIterator interface {
	ForwardBoolIterator

	// SeekForward moves the iterator forward n positions.
	SeekForward(n int) error
}
