package impl

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/pool"
)

var (
	errTimeValuesBuilderAlreadyClosed = errors.New("time values builder is already closed")
)

// ArrayBasedTimeValues is a time values collection backed by an in-memory array.
// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type ArrayBasedTimeValues struct {
	closed   bool
	initDone bool
	min      int64
	max      int64
	vals     *pool.RefCountedPooledInt64Array
}

// NewArrayBasedTimeValues create a new array based time values.
func NewArrayBasedTimeValues(p *pool.BucketizedInt64ArrayPool) *ArrayBasedTimeValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledInt64Array(rawArr, p)
	return &ArrayBasedTimeValues{
		vals: refCountedArr,
	}
}

// Metadata returns the values metadata.
func (b *ArrayBasedTimeValues) Metadata() values.TimeValuesMetadata {
	return values.TimeValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

// Iter returns the values iterator.
func (b *ArrayBasedTimeValues) Iter() (iterator.ForwardTimeIterator, error) {
	return iterator.NewArrayBasedTimeIterator(b.vals.Get()), nil
}

// Add adds a new time value.
func (b *ArrayBasedTimeValues) Add(v int64) error {
	if b.closed {
		return errTimeValuesBuilderAlreadyClosed
	}
	if !b.initDone {
		b.min = v
		b.max = v
		b.initDone = true
	} else {
		if b.min > v {
			b.min = v
		} else if b.max < v {
			b.max = v
		}
	}
	b.vals.Append(v)
	return nil
}

// Snapshot takes a snapshot of the time values.
func (b *ArrayBasedTimeValues) Snapshot() values.CloseableTimeValues {
	return &ArrayBasedTimeValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

// Seal seals the time values builder.
func (b *ArrayBasedTimeValues) Seal() values.CloseableTimeValues {
	sealed := &ArrayBasedTimeValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = ArrayBasedTimeValues{}
	b.Close()

	return sealed
}

// Close closes the time values.
func (b *ArrayBasedTimeValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
