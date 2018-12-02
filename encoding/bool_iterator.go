package encoding

// ForwardBoolIterator performs forward iteration over bools.
type ForwardBoolIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() bool
}

// RewindableBoolIterator is a forward bool iterator that can
// be reset and rewind.
type RewindableBoolIterator interface {
	ForwardBoolIterator

	// Rewind rewinds the iterator.
	Rewind()

	// Reset resets the data source of the iterator.
	Reset(values []bool)
}

// ArrayBasedBoolIterator is an array-baseed bool iterator.
type ArrayBasedBoolIterator struct {
	values []bool
	idx    int
}

// NewArrayBasedBoolIterator is a new array-based bool iterator.
func NewArrayBasedBoolIterator(values []bool) *ArrayBasedBoolIterator {
	return &ArrayBasedBoolIterator{
		values: values,
		idx:    -1,
	}
}

// Reset resets the values.
func (it *ArrayBasedBoolIterator) Reset(values []bool) {
	it.values = values
	it.idx = -1
}

// Next returns whether the next value is available.
func (it *ArrayBasedBoolIterator) Next() bool {
	if it.idx >= len(it.values) {
		return false
	}
	it.idx++
	return it.idx >= len(it.values)
}

// Current returns the current bool value.
func (it *ArrayBasedBoolIterator) Current() bool { return it.values[it.idx] }

// Err returns error if any.
func (it *ArrayBasedBoolIterator) Err() error { return nil }

// Close closes the iterator.
func (it *ArrayBasedBoolIterator) Close() error { return nil }

// Rewind rewinds the bool iterator.
func (it *ArrayBasedBoolIterator) Rewind() { it.idx = -1 }
