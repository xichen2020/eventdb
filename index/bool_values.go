package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/pool"
)

// boolValuesBuilder incrementally builds the bool value collection.
type boolValuesBuilder interface {
	// Add adds a bool to the collection.
	Add(v bool) error

	// Snapshot takes a snapshot of the bool values collected so far.
	Snapshot() values.CloseableBoolValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable bool values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() values.CloseableBoolValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

var (
	errBoolValuesBuilderAlreadyClosed = errors.New("bool values builder is already closed")
)

// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type arrayBasedBoolValues struct {
	closed    bool
	numTrues  int
	numFalses int
	vals      *pool.RefCountedPooledBoolArray
}

func newArrayBasedBoolValues(p *pool.BucketizedBoolArrayPool) *arrayBasedBoolValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledBoolArray(rawArr, p)
	return &arrayBasedBoolValues{
		vals: refCountedArr,
	}
}

func (b *arrayBasedBoolValues) Metadata() values.BoolValuesMetadata {
	return values.BoolValuesMetadata{
		NumTrues:  b.numTrues,
		NumFalses: b.numFalses,
	}
}

func (b *arrayBasedBoolValues) Iter() (iterator.ForwardBoolIterator, error) {
	return iterator.NewArrayBasedBoolIterator(b.vals.Get()), nil
}

func (b *arrayBasedBoolValues) Add(v bool) error {
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

func (b *arrayBasedBoolValues) Snapshot() values.CloseableBoolValues {
	return &arrayBasedBoolValues{
		numTrues:  b.numTrues,
		numFalses: b.numFalses,
		vals:      b.vals.Snapshot(),
	}
}

func (b *arrayBasedBoolValues) Seal() values.CloseableBoolValues {
	sealed := &arrayBasedBoolValues{
		numTrues:  b.numTrues,
		numFalses: b.numFalses,
		vals:      b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = arrayBasedBoolValues{}
	b.Close()

	return sealed
}

func (b *arrayBasedBoolValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
