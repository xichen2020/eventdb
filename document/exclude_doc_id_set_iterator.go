package document

// ExcludeDocIDSetIterator is an iterator that excludes a given doc ID set from
// the full doc ID set defined as [0, numTotalDocs).
type ExcludeDocIDSetIterator struct {
	numTotalDocs int32
	toExcludeIt  DocIDSetIterator

	toExcludeDone bool
	curr          int32
}

// NewExcludeDocIDSetIterator creates a new iterator.
func NewExcludeDocIDSetIterator(
	numTotalDocs int32,
	toExcludeIt DocIDSetIterator,
) *ExcludeDocIDSetIterator {
	toExcludeDone := false
	if !toExcludeIt.Next() {
		toExcludeDone = true
	}
	return &ExcludeDocIDSetIterator{
		numTotalDocs:  numTotalDocs,
		toExcludeIt:   toExcludeIt,
		toExcludeDone: toExcludeDone,
		curr:          -1,
	}
}

// Next returns true if there are more doc IDs to be iterated over.
func (it *ExcludeDocIDSetIterator) Next() bool {
	if it.curr >= it.numTotalDocs {
		return false
	}
	it.curr++
	if it.toExcludeDone {
		return it.curr < it.numTotalDocs
	}
	if it.curr < it.toExcludeIt.DocID() {
		return true
	}
	// The current doc ID is the same as the doc ID to exclude, as such
	// we advance the exclude iterator.
	if !it.toExcludeIt.Next() {
		it.toExcludeDone = true
	}
	return it.Next()
}

// DocID returns the current doc ID.
func (it *ExcludeDocIDSetIterator) DocID() int32 { return it.curr }

// Close closes the iterator.
func (it *ExcludeDocIDSetIterator) Close() {
	it.toExcludeIt.Close()
	it.toExcludeIt = nil
}
