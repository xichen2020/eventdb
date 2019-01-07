package iterator

// ArrayBasedStringIterator is an array-based string iterator.
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
