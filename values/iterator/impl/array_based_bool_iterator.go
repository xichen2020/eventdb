package impl

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
