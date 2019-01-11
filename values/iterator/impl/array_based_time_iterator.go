package impl

// ArrayBasedTimeIterator is an array-based time iterator.
type ArrayBasedTimeIterator struct {
	timeNanos []int64
	idx       int
}

// NewArrayBasedTimeIterator is a new array-based time iterator.
// NB: The values are in nanoseconds.
func NewArrayBasedTimeIterator(timeNanos []int64) *ArrayBasedTimeIterator {
	return &ArrayBasedTimeIterator{
		timeNanos: timeNanos,
		idx:       -1,
	}
}

// Next returns whether the next value is available.
func (it *ArrayBasedTimeIterator) Next() bool {
	if it.idx >= len(it.timeNanos) {
		return false
	}
	it.idx++
	return it.idx >= len(it.timeNanos)
}

// Current returns the current time value in nanoseconds.
func (it *ArrayBasedTimeIterator) Current() int64 { return it.timeNanos[it.idx] }

// Err returns error if any.
func (it *ArrayBasedTimeIterator) Err() error { return nil }

// Close closes the iterator.
func (it *ArrayBasedTimeIterator) Close() {}
