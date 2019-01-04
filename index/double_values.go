package index

import (
	"errors"

	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/x/pool"
)

// DoubleValuesMetadata contains the metadata for the double values collection.
type DoubleValuesMetadata struct {
	Min  float64
	Max  float64
	Size int
}

// DoubleValues is an immutable collection of double values.
type DoubleValues interface {
	// Metadata returns the collection metadata.
	Metadata() DoubleValuesMetadata

	// Iter returns an iterator to provides iterative access to the underlying dataset
	// when the iterator is created. After the iterator is returned, the iterator has
	// no access to future values added to the underlying dataset. The iterator remains
	// valid until the double values collection is closed.
	// TODO(xichen): Change this to `ForwardDoubleIterator`.
	Iter() encoding.RewindableDoubleIterator
}

type closeableDoubleValues interface {
	DoubleValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// doubleValuesBuilder incrementally builds the double value collection.
type doubleValuesBuilder interface {
	// Add adds a double to the collection.
	Add(v float64) error

	// Snapshot takes a snapshot of the double values collected so far.
	Snapshot() closeableDoubleValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable double values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() closeableDoubleValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

var (
	errDoubleValuesBuilderAlreadyClosed = errors.New("double values builder is already closed")
)

// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type arrayBasedDoubleValues struct {
	closed   bool
	initDone bool
	min      float64
	max      float64
	vals     *pool.RefCountedPooledFloat64Array
}

func newArrayBasedDoubleValues(p *pool.BucketizedFloat64ArrayPool) *arrayBasedDoubleValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledFloat64Array(rawArr, p)
	return &arrayBasedDoubleValues{
		vals: refCountedArr,
	}
}

func (b *arrayBasedDoubleValues) Metadata() DoubleValuesMetadata {
	return DoubleValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

func (b *arrayBasedDoubleValues) Iter() encoding.RewindableDoubleIterator {
	return encoding.NewArrayBasedDoubleIterator(b.vals.Get())
}

func (b *arrayBasedDoubleValues) Add(v float64) error {
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

func (b *arrayBasedDoubleValues) Snapshot() closeableDoubleValues {
	return &arrayBasedDoubleValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

func (b *arrayBasedDoubleValues) Seal() closeableDoubleValues {
	sealed := &arrayBasedDoubleValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = arrayBasedDoubleValues{}
	b.Close()

	return sealed
}

func (b *arrayBasedDoubleValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
