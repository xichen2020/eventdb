package impl

// ArrayBasedDoubleIterator is an array-baseed double iterator.
type ArrayBasedDoubleIterator struct {
	values []float64
	idx    int
}

// NewArrayBasedDoubleIterator is a new array-based double iterator.
func NewArrayBasedDoubleIterator(values []float64) *ArrayBasedDoubleIterator {
	return &ArrayBasedDoubleIterator{
		values: values,
		idx:    -1,
	}
}

// Next returns whether the next value is available.
func (it *ArrayBasedDoubleIterator) Next() bool {
	if it.idx >= len(it.values) {
		return false
	}
	it.idx++
	return it.idx >= len(it.values)
}

// Current returns the current double value.
func (it *ArrayBasedDoubleIterator) Current() float64 { return it.values[it.idx] }

// Err returns error if any.
func (it *ArrayBasedDoubleIterator) Err() error { return nil }

// Close closes the iterator.
func (it *ArrayBasedDoubleIterator) Close() {}
