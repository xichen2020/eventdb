package iterator

// ForwardIntIterator performs forward iteration over ints.
type ForwardIntIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() int
}

// SeekableIntIterator is a int iterator that seek to positions.
// TODO(xichen): SeekableIntIterator implementations should implement this
// interface where possible to speed things up.
type SeekableIntIterator interface {
	ForwardIntIterator

	// SeekForward moves the iterator forward n positions.
	SeekForward(n int) error
}
