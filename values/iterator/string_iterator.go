package iterator

// ForwardStringIterator performs forward iteration over strings.
type ForwardStringIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() string
}
