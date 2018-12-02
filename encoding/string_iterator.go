package encoding

// ForwardStringIterator performs forward iteration over strings.
type ForwardStringIterator interface {
	baseIterator

	// Current returns the current value in the iteration.
	Current() string
}

// RewindableStringIterator is a forward string iterator that can
// be reset and rewind.
type RewindableStringIterator interface {
	ForwardStringIterator

	// Rewind rewinds the iterator.
	Rewind()

	// Reset resets the data source of the iterator.
	Reset(values []string)
}

// ArrayBasedStringIterator is an array-baseed string iterator.
type ArrayBasedStringIterator struct {
	values []string
	idx    int
}

// NewArrayBasedStringIterator is a new array-based string iterator.
func NewArrayBasedStringIterator(values []string) *ArrayBasedStringIterator {
	return &ArrayBasedStringIterator{
		values: values,
		idx:    -1,
	}
}

// Reset resets the values.
func (it *ArrayBasedStringIterator) Reset(values []string) {
	it.values = values
	it.idx = -1
}

// Next returns whether the next value is available.
func (it *ArrayBasedStringIterator) Next() bool {
	if it.idx >= len(it.values) {
		return false
	}
	it.idx++
	return it.idx >= len(it.values)
}

// Current returns the current string value.
func (it *ArrayBasedStringIterator) Current() string { return it.values[it.idx] }

// Err returns error if any.
func (it *ArrayBasedStringIterator) Err() error { return nil }

// Close closes the iterator.
func (it *ArrayBasedStringIterator) Close() error { return nil }

// Rewind rewinds the string iterator.
func (it *ArrayBasedStringIterator) Rewind() { it.idx = -1 }
