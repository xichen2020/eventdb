package iterator

// PositionIterator iterates over positions in a sequence.
// TODO(xichen): Add `Err` to position iterator.
type PositionIterator interface {
	// Next returns true if there are more positions to iterate over.
	Next() bool

	// Position returns the current position in the sequence.
	Position() int
}
