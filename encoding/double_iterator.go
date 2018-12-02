package encoding

// ForwardDoubleIterator performs forward iteration over doubles.
type ForwardDoubleIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() float64
}

// RewindableDoubleIterator is a forward double iterator that can
// be reset and rewind.
type RewindableDoubleIterator interface {
	ForwardDoubleIterator

	// Rewind rewinds the iterator.
	Rewind()

	// Reset resets the data source of the iterator.
	Reset(values []float64)
}

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

// Reset resets the values.
func (it *ArrayBasedDoubleIterator) Reset(values []float64) {
	it.values = values
	it.idx = -1
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
func (it *ArrayBasedDoubleIterator) Close() error { return nil }

// Rewind rewinds the double iterator.
func (it *ArrayBasedDoubleIterator) Rewind() { it.idx = -1 }
