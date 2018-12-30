package document

// inAllDocIDSetIterIterator iterates over an array of doc ID set iterators.
// Each inner iterator outputs doc IDs in increasing order, and the outer
// iterator only outputs a doc ID if it exists in the output of all inner iterators.
// TODO(xichen): Look into using bitset to make this faster based on benchmark results.
type inAllDocIDSetIterIterator struct {
	iters []DocIDSetIterator

	done   bool
	docIDs []int32
}

// nolint: deadcode
// TODO(xichen): Remove the nolint directive once this is used.
func newInAllDocIDSetIterIterator(iters ...DocIDSetIterator) *inAllDocIDSetIterIterator {
	return &inAllDocIDSetIterIterator{
		iters:  iters,
		docIDs: make([]int32, len(iters)),
	}
}

// NB(xichen): This has a complexity of O(MN) where M is the
// number of inner iterators and N is the total number of document
// IDs across all iterators. However in practice, M is likely
// to be small so this may end up being as fast as or faster than
// a min heap based solution.
func (it *inAllDocIDSetIterIterator) Next() bool {
	if it.done || len(it.iters) == 0 {
		return false
	}
	// Advance all iterators first.
	for i, iit := range it.iters {
		if !iit.Next() {
			it.done = true
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
			return false
		}
		it.docIDs[minIdx] = it.iters[minIdx].DocID()
	}
}

func (it *inAllDocIDSetIterIterator) DocID() int32 {
	return it.docIDs[0]
}

func (it *inAllDocIDSetIterIterator) Close() {
	for _, iit := range it.iters {
		iit.Close()
	}
	it.iters = nil
}
