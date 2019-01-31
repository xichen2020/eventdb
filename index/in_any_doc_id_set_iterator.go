package index

const (
	invalidDocID int32 = -1
)

// InAnyDocIDSetIterator iterates over an array of doc ID set iterators.
// Each inner iterator outputs doc IDs in increasing order, and the outer
// iterator outputs a doc ID if it exists in the output of any inner iterators.
// TODO(xichen): Look into using bitset to make this faster based on benchmark results.
type InAnyDocIDSetIterator struct {
	iters []DocIDSetIterator

	done        bool
	docIDs      []int32
	prevDocID   int32
	minDocIDIdx int
}

// NewInAnyDocIDSetIterator creates a new iterator.
func NewInAnyDocIDSetIterator(iters ...DocIDSetIterator) *InAnyDocIDSetIterator {
	return &InAnyDocIDSetIterator{
		iters:       iters,
		done:        len(iters) == 0,
		docIDs:      make([]int32, len(iters)),
		prevDocID:   invalidDocID,
		minDocIDIdx: -1,
	}
}

// Next returns true if there are more doc IDs to be iterated over.
// NB(xichen): This has a complexity of O(MN) where M is the
// number of inner iterators and N is the total number of document
// IDs across all iterators. However in practice, M is likely
// to be small so this may end up being as fast as or faster than
// a min heap based solution.
func (it *InAnyDocIDSetIterator) Next() bool {
	if it.done {
		return false
	}
	if it.prevDocID == invalidDocID {
		// Advance all iterators first if this is the first time.
		for i, iit := range it.iters {
			if iit.Next() {
				it.docIDs[i] = iit.DocID()
			} else {
				it.docIDs[i] = invalidDocID
			}
		}
	} else {
		// Advance the iterator with the smallest doc ID.
		if it.iters[it.minDocIDIdx].Next() {
			it.docIDs[it.minDocIDIdx] = it.iters[it.minDocIDIdx].DocID()
		} else {
			it.docIDs[it.minDocIDIdx] = invalidDocID
		}
	}

	// Find the smallest index.
	minDocIDIdx := -1
	for i, docID := range it.docIDs {
		if docID == invalidDocID {
			continue
		}
		if minDocIDIdx == -1 || it.docIDs[minDocIDIdx] > docID {
			minDocIDIdx = i
		}
	}
	if minDocIDIdx == -1 {
		// No valid doc ID found, return.
		it.done = true
		return false
	}

	it.minDocIDIdx = minDocIDIdx
	currDocID := it.docIDs[minDocIDIdx]
	if it.prevDocID == invalidDocID || it.prevDocID != currDocID {
		it.prevDocID = currDocID
		return true
	}
	// Duplicate doc ID found, recurse.
	return it.Next()
}

// DocID returns the current doc ID.
func (it *InAnyDocIDSetIterator) DocID() int32 {
	return it.docIDs[it.minDocIDIdx]
}

// IsDoneAt returns whether the iterator at a given index is done.
func (it *InAnyDocIDSetIterator) IsDoneAt(idx int) bool {
	return it.docIDs[idx] == invalidDocID
}

// Err returns any errors encountered.
// TODO(xichen): Implement this.
func (it *InAnyDocIDSetIterator) Err() error {
	return nil
}

// Close closes the iterator.
func (it *InAnyDocIDSetIterator) Close() {
	for i := range it.iters {
		it.iters[i].Close()
		it.iters[i] = nil
	}
	it.iters = nil
}
