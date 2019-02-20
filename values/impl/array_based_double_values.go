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
	errDoubleValuesBuilderAlreadyClosed = errors.New("double values builder is already closed")
)

// ArrayBasedDoubleValues is a double values collection backed by an in-memory array.
// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type ArrayBasedDoubleValues struct {
	closed   bool
	initDone bool
	min      float64
	max      float64
	vals     *pool.RefCountedPooledFloat64Array
}

// NewArrayBasedDoubleValues create a new array based double values.
func NewArrayBasedDoubleValues(p *pool.BucketizedFloat64ArrayPool) *ArrayBasedDoubleValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledFloat64Array(rawArr, p, nil)
	return &ArrayBasedDoubleValues{
		vals: refCountedArr,
	}
}

// Metadata returns the values metadata.
func (b *ArrayBasedDoubleValues) Metadata() values.DoubleValuesMetadata {
	return values.DoubleValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

// Iter returns the values iterator.
func (b *ArrayBasedDoubleValues) Iter() (iterator.ForwardDoubleIterator, error) {
	return iterimpl.NewArrayBasedDoubleIterator(b.vals.Get()), nil
}

// Filter applies the given filter against the values, returning an iterator
// identifying the positions of values matching the filter.
func (b *ArrayBasedDoubleValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredArrayBasedDoubleValueIterator(b, op, filterValue)
}

// Add adds a new double value.
func (b *ArrayBasedDoubleValues) Add(v float64) error {
	if b.closed {
		return errDoubleValuesBuilderAlreadyClosed
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

// Snapshot takes a snapshot of the double values.
func (b *ArrayBasedDoubleValues) Snapshot() values.CloseableDoubleValues {
	return &ArrayBasedDoubleValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

// Seal seals the double values builder.
func (b *ArrayBasedDoubleValues) Seal() values.CloseableDoubleValues {
	sealed := &ArrayBasedDoubleValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = ArrayBasedDoubleValues{}
	b.Close()

	return sealed
}

// Close closes the double values.
func (b *ArrayBasedDoubleValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
