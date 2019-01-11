package index

// DocIDSetIterator is the document ID set iterator.
// TODO(xichen): Add `Err` API.
type DocIDSetIterator interface {
	// Next returns true if there are more document IDs to be iterated over.
	Next() bool

	// DocID returns the current document ID.
	// NB: This is not called `Current` because it needs to
	// be embedded with other iterators so the method name is
	// more specific w.r.t. what value this is referring to.
	DocID() int32

	// Close closes the iterator.
	Close()
}

// SeekableDocIDSetIterator is a doc ID set iterator that can seek to positions.
// TODO(xichen): DocIDSetIterator implementations should implement this interface
// where possible to speed things up.
type SeekableDocIDSetIterator interface {
	DocIDSetIterator

	// SeekForward moves the iterator forward n positions.
	SeekForward(n int) error
}

// DocIDSetIteratorFn transforms an input doc ID set iterator into a new doc ID set iterator.
type DocIDSetIteratorFn func(it DocIDSetIterator) DocIDSetIterator

// NoOpDocIDSetIteratorFn is a no op transformation function that returns the input iterator as is.
func NoOpDocIDSetIteratorFn(it DocIDSetIterator) DocIDSetIterator { return it }

// ExcludeDocIDSetIteratorFn returns a transformation function that excludes the doc ID set
// associated with the input iterator from the full doc ID set.
func ExcludeDocIDSetIteratorFn(numTotalDocs int32) DocIDSetIteratorFn {
	return func(it DocIDSetIterator) DocIDSetIterator {
		return NewExcludeDocIDSetIterator(numTotalDocs, it)
	}
}
