package iterator

// ForwardIntIterator performs forward iteration over ints.
type ForwardIntIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() int
}
