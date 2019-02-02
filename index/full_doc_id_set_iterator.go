package index

// FullDocIDSetIterator is an iterator for a full doc ID set containing document IDs
// ranging from 0 (inclusive) to `numTotalDocs` (exclusive).
type FullDocIDSetIterator struct {
	numTotalDocs int32

	curr int32
}

// NewFullDocIDSetIterator creates a new full doc ID set iterator.
func NewFullDocIDSetIterator(numTotalDocs int32) *FullDocIDSetIterator {
	return &FullDocIDSetIterator{numTotalDocs: numTotalDocs, curr: -1}
}

// Next returns true if there are more doc IDs to be iterated over.
func (it *FullDocIDSetIterator) Next() bool {
	if it.curr >= it.numTotalDocs {
		return false
	}
	it.curr++
	return it.curr < it.numTotalDocs
}

// DocID returns the current doc ID.
func (it *FullDocIDSetIterator) DocID() int32 { return it.curr }

// Err returns any error encountered during iteration.
func (it *FullDocIDSetIterator) Err() error { return nil }

// Close closes the iterator.
func (it *FullDocIDSetIterator) Close() {}
