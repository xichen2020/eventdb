package impl

import "github.com/xichen2020/eventdb/x/bytes"

// ArrayBasedBytesIterator is an array-based bytes iterator.
type ArrayBasedBytesIterator struct {
	values [][]byte
	idx    int
}

// NewArrayBasedBytesIterator is a new array-based bytes iterator.
func NewArrayBasedBytesIterator(values [][]byte) *ArrayBasedBytesIterator {
	return &ArrayBasedBytesIterator{
		values: values,
		idx:    -1,
	}
}

// Next returns whether the next value is available.
func (it *ArrayBasedBytesIterator) Next() bool {
	if it.idx >= len(it.values) {
		return false
	}
	it.idx++
	return it.idx < len(it.values)
}

// Current returns the current bytes value.
func (it *ArrayBasedBytesIterator) Current() bytes.Bytes {
	return bytes.NewImmutableBytes(it.values[it.idx])
}

// Err returns error if any.
func (it *ArrayBasedBytesIterator) Err() error { return nil }

// Close closes the iterator.
func (it *ArrayBasedBytesIterator) Close() {}
