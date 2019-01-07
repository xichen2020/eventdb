package iterator

// ForwardBoolIterator performs forward iteration over bools.
type ForwardBoolIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() bool
}
