package iterator

// ForwardTimeIterator performs forward iteration over times.
type ForwardTimeIterator interface {
	baseIterator

	// Current returns the current value in nanoseconds in the iteration.
	Current() int64
}

// SeekableTimeIterator is a time iterator that seek to positions.
// TODO(xichen): SeekableTimeIterator implementations should implement this
// interface where possible to speed things up.
type SeekableTimeIterator interface {
	ForwardTimeIterator

	// SeekForward moves the iterator forward n positions.
	SeekForward(n int) error
}
