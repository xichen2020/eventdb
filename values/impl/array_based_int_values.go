package impl

import (
	"errors"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	iterimpl "github.com/xichen2020/eventdb/values/iterator/impl"
	"github.com/xichen2020/eventdb/x/pool"
)

var (
	errIntValuesBuilderAlreadyClosed = errors.New("int values builder is already closed")
)

// ArrayBasedIntValues is a int values collection backed by an in-memory array.
// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type ArrayBasedIntValues struct {
	closed   bool
	initDone bool
	min      int
	max      int
	vals     *pool.RefCountedPooledIntArray
}

// NewArrayBasedIntValues create a new array based int values.
func NewArrayBasedIntValues(p *pool.BucketizedIntArrayPool) *ArrayBasedIntValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledIntArray(rawArr, p, nil)
	return &ArrayBasedIntValues{
		vals: refCountedArr,
	}
}

// Metadata returns the values metadata.
func (b *ArrayBasedIntValues) Metadata() values.IntValuesMetadata {
	return values.IntValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

// Iter returns the values iterator.
func (b *ArrayBasedIntValues) Iter() (iterator.ForwardIntIterator, error) {
	return iterimpl.NewArrayBasedIntIterator(b.vals.Get()), nil
}

// Filter applies the given filter against the values, returning an iterator
// identifying the positions of values matching the filter.
func (b *ArrayBasedIntValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredArrayBasedIntValueIterator(b, op, filterValue)
}

// Add adds a new int value.
func (b *ArrayBasedIntValues) Add(v int) error {
	if b.closed {
		return errIntValuesBuilderAlreadyClosed
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

// Snapshot takes a snapshot of the int values.
func (b *ArrayBasedIntValues) Snapshot() values.CloseableIntValues {
	return &ArrayBasedIntValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

// Seal seals the int values builder.
func (b *ArrayBasedIntValues) Seal() values.CloseableIntValues {
	sealed := &ArrayBasedIntValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = ArrayBasedIntValues{}
	b.Close()

	return sealed
}

// Close closes the int values.
func (b *ArrayBasedIntValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
