package template

// FilteredValueIterator is a position iterator that outputs the positions
// of values in the value sequence matching a given filter. The position starts at 0.
type FilteredValueIterator struct {
	vit ForwardValueIterator
	f   ValueFilter

	done    bool
	currPos int
	err     error
}

// NewFilteredValueIterator creates a new filtering iterator.
func NewFilteredValueIterator(
	vit ForwardValueIterator,
	f ValueFilter,
) *FilteredValueIterator {
	return &FilteredValueIterator{
		vit:     vit,
		f:       f,
		currPos: -1,
	}
}

// Next returns true if there are more values to be iterated over.
func (it *FilteredValueIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	for it.vit.Next() {
		it.currPos++
		if it.f.Match(it.vit.Current()) {
			return true
		}
	}
	it.done = true
	it.err = it.vit.Err()
	return false
}

// Position returns the current position.
func (it *FilteredValueIterator) Position() int { return it.currPos }

// Err returns any errors encountered during iteration.
func (it *FilteredValueIterator) Err() error { return it.err }

// Close closes the iterator.
func (it *FilteredValueIterator) Close() {
	it.vit.Close()
	it.vit = nil
	it.f = nil
}
