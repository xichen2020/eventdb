package iterator

// ForwardDoubleIterator performs forward iteration over doubles.
type ForwardDoubleIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() float64
}
