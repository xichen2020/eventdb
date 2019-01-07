package iterator

// ForwardTimeIterator performs forward iteration over times.
type ForwardTimeIterator interface {
	baseIterator

	// Current returns the current value in nanoseconds in the iteration.
	Current() int64
}
