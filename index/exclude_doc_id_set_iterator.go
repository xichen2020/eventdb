package index

// ExcludeDocIDSetIterator is an iterator that excludes a given doc ID set from
// the full doc ID set defined as [0, numTotalDocs).
type ExcludeDocIDSetIterator struct {
	numTotalDocs int32
	toExcludeIt  DocIDSetIterator

	toExcludeDone bool
	curr          int32
	err           error
}

// NewExcludeDocIDSetIterator creates a new iterator.
func NewExcludeDocIDSetIterator(
	numTotalDocs int32,
	toExcludeIt DocIDSetIterator,
) *ExcludeDocIDSetIterator {
	var (
		toExcludeDone bool
		err           error
	)
	if !toExcludeIt.Next() {
		toExcludeDone = true
		err = toExcludeIt.Err()
	}
	return &ExcludeDocIDSetIterator{
		numTotalDocs:  numTotalDocs,
		toExcludeIt:   toExcludeIt,
		toExcludeDone: toExcludeDone,
		curr:          -1,
		err:           err,
	}
}

// Next returns true if there are more doc IDs to be iterated over.
func (it *ExcludeDocIDSetIterator) Next() bool {
	if it.err != nil || it.curr >= it.numTotalDocs {
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
		it.err = it.toExcludeIt.Err()
	}
	return it.Next()
}

// DocID returns the current doc ID.
func (it *ExcludeDocIDSetIterator) DocID() int32 { return it.curr }

// Err returns any error encountered during iteration.
func (it *ExcludeDocIDSetIterator) Err() error { return it.err }

// Close closes the iterator.
func (it *ExcludeDocIDSetIterator) Close() {
	it.toExcludeIt.Close()
	it.toExcludeIt = nil
	it.err = nil
}
