package impl

import (
	"errors"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	iterimpl "github.com/xichen2020/eventdb/values/iterator/impl"
	"github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/pool"
)

var (
	errBytesValuesBuilderAlreadyClosed = errors.New("bytes values builder is already closed")
)

// ArrayBasedBytesValues is a bytes values collection backed by an in-memory array.
// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type ArrayBasedBytesValues struct {
	closed   bool
	initDone bool
	min      []byte
	max      []byte
	vals     *pool.RefCountedPooledBytesArray
}

// NewArrayBasedBytesValues create a new array based byts values.
func NewArrayBasedBytesValues(
	p *pool.BucketizedBytesArrayPool,
	bytesArrayResetFn bytes.ArrayFn,
) *ArrayBasedBytesValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledBytesArray(rawArr, p, bytesArrayResetFn)
	return &ArrayBasedBytesValues{
		vals: refCountedArr,
	}
}

// Metadata returns the values metadata.
func (b *ArrayBasedBytesValues) Metadata() values.BytesValuesMetadata {
	return values.BytesValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

// Iter returns the values iterator.
func (b *ArrayBasedBytesValues) Iter() (iterator.ForwardBytesIterator, error) {
	return iterimpl.NewArrayBasedBytesIterator(b.vals.Get()), nil
}

// Filter applies the given filter against the values, returning an iterator
// identifying the positions of values matching the filter.
func (b *ArrayBasedBytesValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredArrayBasedBytesValueIterator(b, op, filterValue)
}

// Add adds a new bytes value.
func (b *ArrayBasedBytesValues) Add(v []byte) error {
	if b.closed {
		return errBytesValuesBuilderAlreadyClosed
	}
	if !b.initDone {
		b.min = v
		b.max = v
		b.initDone = true
	} else {
		if bytes.GreaterThan(b.min, v) {
			b.min = v
		} else if bytes.LessThan(b.max, v) {
			b.max = v
		}
	}
	b.vals.Append(v)
	return nil
}

// Snapshot takes a snapshot of the bytes values.
func (b *ArrayBasedBytesValues) Snapshot() values.CloseableBytesValues {
	return &ArrayBasedBytesValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

// Seal seals the bytes values builder.
func (b *ArrayBasedBytesValues) Seal() values.CloseableBytesValues {
	sealed := &ArrayBasedBytesValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = ArrayBasedBytesValues{}
	b.Close()

	return sealed
}

// Close closes the bytes values.
func (b *ArrayBasedBytesValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
