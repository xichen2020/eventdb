package index

import (
	"errors"

	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/x/pool"
)

// IntValuesMetadata contains the metadata for the int values collection.
type IntValuesMetadata struct {
	Min  int
	Max  int
	Size int
}

// IntValues is an immutable collection of int values.
type IntValues interface {
	// Metadata returns the collection metadata.
	Metadata() IntValuesMetadata

	// Iter returns an iterator to provides iterative access to the underlying dataset
	// when the iterator is created. After the iterator is returned, the iterator has
	// no access to future values added to the underlying dataset. The iterator remains
	// valid until the int values collection is closed.
	// TODO(xichen): Change this to `ForwardIntIterator`.
	Iter() encoding.RewindableIntIterator
}

type closeableIntValues interface {
	IntValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// intValuesBuilder incrementally builds the int value collection.
type intValuesBuilder interface {
	// Add adds a int to the collection.
	Add(v int) error

	// Snapshot takes a snapshot of the int values collected so far.
	Snapshot() closeableIntValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable int values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() closeableIntValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

var (
	errIntValuesBuilderAlreadyClosed = errors.New("int values builder is already closed")
)

// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type arrayBasedIntValues struct {
	closed   bool
	initDone bool
	min      int
	max      int
	vals     *pool.RefCountedPooledIntArray
}

func newArrayBasedIntValues(p *pool.BucketizedIntArrayPool) *arrayBasedIntValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledIntArray(rawArr, p)
	return &arrayBasedIntValues{
		vals: refCountedArr,
	}
}

func (b *arrayBasedIntValues) Metadata() IntValuesMetadata {
	return IntValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

func (b *arrayBasedIntValues) Iter() encoding.RewindableIntIterator {
	return encoding.NewArrayBasedIntIterator(b.vals.Get())
}

func (b *arrayBasedIntValues) Add(v int) error {
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

func (b *arrayBasedIntValues) Snapshot() closeableIntValues {
	return &arrayBasedIntValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

func (b *arrayBasedIntValues) Seal() closeableIntValues {
	sealed := &arrayBasedIntValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = arrayBasedIntValues{}
	b.Close()

	return sealed
}

func (b *arrayBasedIntValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}