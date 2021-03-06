package index

// EmptyDocIDSetIterator is an empty doc ID set iterator.
type EmptyDocIDSetIterator struct{}

// NewEmptyDocIDSetIterator creates a new empty doc ID set iterator.
func NewEmptyDocIDSetIterator() EmptyDocIDSetIterator {
	return EmptyDocIDSetIterator{}
}

// Next returns true if there are more doc IDs to be iterated over.
func (it EmptyDocIDSetIterator) Next() bool { return false }

// DocID returns the current doc ID.
func (it EmptyDocIDSetIterator) DocID() int32 { return invalidDocID }

// Err returns any error encountered during iteration.
func (it EmptyDocIDSetIterator) Err() error { return nil }

// Close closes the iterator.
func (it EmptyDocIDSetIterator) Close() {}
