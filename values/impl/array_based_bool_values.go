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
	errBoolValuesBuilderAlreadyClosed = errors.New("bool values builder is already closed")
)

// ArrayBasedBoolValues is a bool values collection backed by an in-memory array.
// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type ArrayBasedBoolValues struct {
	closed    bool
	numTrues  int
	numFalses int
	vals      *pool.RefCountedPooledBoolArray
}

// NewArrayBasedBoolValues create a new array based bool values.
func NewArrayBasedBoolValues(p *pool.BucketizedBoolArrayPool) *ArrayBasedBoolValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledBoolArray(rawArr, p, nil)
	return &ArrayBasedBoolValues{
		vals: refCountedArr,
	}
}

// Metadata returns the values metadata.
func (b *ArrayBasedBoolValues) Metadata() values.BoolValuesMetadata {
	return values.BoolValuesMetadata{
		NumTrues:  b.numTrues,
		NumFalses: b.numFalses,
	}
}

// Iter returns the values iterator.
func (b *ArrayBasedBoolValues) Iter() (iterator.ForwardBoolIterator, error) {
	return iterimpl.NewArrayBasedBoolIterator(b.vals.Get()), nil
}

// Filter applies the given filter against the values, returning an iterator
// identifying the positions of values matching the filter.
func (b *ArrayBasedBoolValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredArrayBasedBoolValueIterator(b, op, filterValue)
}

// Add adds a new bool value.
func (b *ArrayBasedBoolValues) Add(v bool) error {
	if b.closed {
		return errBoolValuesBuilderAlreadyClosed
	}
	if v {
		b.numTrues++
	} else {
		b.numFalses++
	}
	b.vals.Append(v)
	return nil
}

// Snapshot takes a snapshot of the bool values.
func (b *ArrayBasedBoolValues) Snapshot() values.CloseableBoolValues {
	return &ArrayBasedBoolValues{
		numTrues:  b.numTrues,
		numFalses: b.numFalses,
		vals:      b.vals.Snapshot(),
	}
}

// Seal seals the bool values builder.
func (b *ArrayBasedBoolValues) Seal() values.CloseableBoolValues {
	sealed := &ArrayBasedBoolValues{
		numTrues:  b.numTrues,
		numFalses: b.numFalses,
		vals:      b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = ArrayBasedBoolValues{}
	b.Close()

	return sealed
}

// Close closes the bool values.
func (b *ArrayBasedBoolValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
