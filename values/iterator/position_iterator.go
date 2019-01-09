package iterator

// PositionIterator iterates over positions in a sequence.
type PositionIterator interface {
	// Next returns true if there are more positions to iterate over.
	Next() bool

	// Current returns the current position in the sequence.
	Current() int
}
