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
	errStringValuesBuilderAlreadyClosed = errors.New("string values builder is already closed")
)

// ArrayBasedStringValues is a string values collection backed by an in-memory array.
// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type ArrayBasedStringValues struct {
	closed   bool
	initDone bool
	min      string
	max      string
	vals     *pool.RefCountedPooledStringArray
}

// NewArrayBasedStringValues create a new array based string values.
func NewArrayBasedStringValues(p *pool.BucketizedStringArrayPool, valuesResetFn *func([]string)) *ArrayBasedStringValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledStringArray(rawArr, p, valuesResetFn)
	return &ArrayBasedStringValues{
		vals: refCountedArr,
	}
}

// Metadata returns the values metadata.
func (b *ArrayBasedStringValues) Metadata() values.StringValuesMetadata {
	return values.StringValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

// Iter returns the values iterator.
func (b *ArrayBasedStringValues) Iter() (iterator.ForwardStringIterator, error) {
	return iterimpl.NewArrayBasedStringIterator(b.vals.Get()), nil
}

// Filter applies the given filter against the values, returning an iterator
// identifying the positions of values matching the filter.
func (b *ArrayBasedStringValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredArrayBasedStringValueIterator(b, op, filterValue)
}

// Add adds a new string value.
func (b *ArrayBasedStringValues) Add(v string) error {
	if b.closed {
		return errStringValuesBuilderAlreadyClosed
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

// Snapshot takes a snapshot of the string values.
func (b *ArrayBasedStringValues) Snapshot() values.CloseableStringValues {
	return &ArrayBasedStringValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

// Seal seals the string values builder.
func (b *ArrayBasedStringValues) Seal() values.CloseableStringValues {
	sealed := &ArrayBasedStringValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = ArrayBasedStringValues{}
	b.Close()

	return sealed
}

// Close closes the string values.
func (b *ArrayBasedStringValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
