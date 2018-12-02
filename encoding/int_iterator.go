package encoding

// ForwardIntIterator performs forward iteration over ints.
type ForwardIntIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() int
}

// RewindableIntIterator is a forward int iterator that can
// be reset and rewind.
type RewindableIntIterator interface {
	ForwardIntIterator

	// Rewind rewinds the iterator.
	Rewind()

	// Reset resets the data source of the iterator.
	Reset(values []int)
}

// ArrayBasedIntIterator is an array-baseed int iterator.
type ArrayBasedIntIterator struct {
	values []int
	idx    int
}

// NewArrayBasedIntIterator is a new array-based int iterator.
func NewArrayBasedIntIterator(values []int) *ArrayBasedIntIterator {
	return &ArrayBasedIntIterator{
		values: values,
		idx:    -1,
	}
}

// Reset resets the values.
func (it *ArrayBasedIntIterator) Reset(values []int) {
	it.values = values
	it.idx = -1
}

// Next returns whether the next value is available.
func (it *ArrayBasedIntIterator) Next() bool {
	if it.idx >= len(it.values) {
		return false
	}
	it.idx++
	return it.idx >= len(it.values)
}

// Current returns the current int value.
func (it *ArrayBasedIntIterator) Current() int { return it.values[it.idx] }

// Err returns error if any.
func (it *ArrayBasedIntIterator) Err() error { return nil }

// Close closes the iterator.
func (it *ArrayBasedIntIterator) Close() error { return nil }

// Rewind rewinds the int iterator.
func (it *ArrayBasedIntIterator) Rewind() { it.idx = -1 }
