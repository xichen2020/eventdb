package index

import (
	"errors"

	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/x/pool"
)

// BoolValuesMetadata contains the metadata for the bool values collection.
type BoolValuesMetadata struct {
	NumTrues  int
	NumFalses int
	Size      int
}

// BoolValues is an immutable collection of bool values.
type BoolValues interface {
	// Metadata returns the collection metadata.
	Metadata() BoolValuesMetadata

	// Iter returns an iterator to provides iterative access to the underlying dataset
	// when the iterator is created. After the iterator is returned, the iterator has
	// no access to future values added to the underlying dataset. The iterator remains
	// valid until the bool values collection is closed.
	// TODO(xichen): Change this to `ForwardBoolIterator`.
	Iter() encoding.RewindableBoolIterator
}

type closeableBoolValues interface {
	BoolValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// boolValuesBuilder incrementally builds the bool value collection.
type boolValuesBuilder interface {
	// Add adds a bool to the collection.
	Add(v bool) error

	// Snapshot takes a snapshot of the bool values collected so far.
	Snapshot() closeableBoolValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable bool values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() closeableBoolValues

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

func (b *arrayBasedBoolValues) Metadata() BoolValuesMetadata {
	return BoolValuesMetadata{
		NumTrues:  b.numTrues,
		NumFalses: b.numFalses,
		Size:      len(b.vals.Get()),
	}
}

func (b *arrayBasedBoolValues) Iter() encoding.RewindableBoolIterator {
	return encoding.NewArrayBasedBoolIterator(b.vals.Get())
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

func (b *arrayBasedBoolValues) Snapshot() closeableBoolValues {
	return &arrayBasedBoolValues{
		numTrues:  b.numTrues,
		numFalses: b.numFalses,
		vals:      b.vals.Snapshot(),
	}
}

func (b *arrayBasedBoolValues) Seal() closeableBoolValues {
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
