package index

// InAllDocIDSetIterator iterates over an array of doc ID set iterators.
// Each inner iterator outputs doc IDs in increasing order, and the outer
// iterator only outputs a doc ID if it exists in the output of all inner iterators.
// TODO(xichen): Look into using bitset to make this faster based on benchmark results.
type InAllDocIDSetIterator struct {
	iters []DocIDSetIterator

	done   bool
	docIDs []int32
	err    error
}

// NewInAllDocIDSetIterator creates a new iterator.
func NewInAllDocIDSetIterator(iters ...DocIDSetIterator) *InAllDocIDSetIterator {
	return &InAllDocIDSetIterator{
		iters:  iters,
		docIDs: make([]int32, len(iters)),
		done:   len(iters) == 0,
	}
}

// Next returns true if there are more doc IDs to be iterated over.
// NB(xichen): This has a complexity of O(MN) where M is the
// number of inner iterators and N is the total number of document
// IDs across all iterators. However in practice, M is likely
// to be small so this may end up being as fast as or faster than
// a min heap based solution.
func (it *InAllDocIDSetIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	// Advance all iterators first.
	for i, iit := range it.iters {
		if !iit.Next() {
			it.done = true
			it.err = iit.Err()
			return false
		}
		it.docIDs[i] = iit.DocID()
	}
	for {
		var minIdx, maxIdx int
		for i := 1; i < len(it.docIDs); i++ {
			if it.docIDs[i] < it.docIDs[minIdx] {
				minIdx = i
			}
			if it.docIDs[i] > it.docIDs[maxIdx] {
				maxIdx = i
			}
		}
		// All iterators have the same doc ID.
		if it.docIDs[minIdx] == it.docIDs[maxIdx] {
			return true
		}
		if !it.iters[minIdx].Next() {
			it.done = true
			it.err = it.iters[minIdx].Err()
			return false
		}
		it.docIDs[minIdx] = it.iters[minIdx].DocID()
	}
}

// DocID returns the current doc ID.
func (it *InAllDocIDSetIterator) DocID() int32 {
	return it.docIDs[0]
}

// Err returns any errors encountered.
func (it *InAllDocIDSetIterator) Err() error { return it.err }

// Close closes the iterator.
func (it *InAllDocIDSetIterator) Close() {
	for i := range it.iters {
		it.iters[i].Close()
		it.iters[i] = nil
	}
	it.iters = nil
	it.err = nil
}
