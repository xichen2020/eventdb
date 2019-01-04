package index

import (
	"errors"

	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/x/pool"
)

// TimeValuesMetadata contains the metadata for the time values collection.
type TimeValuesMetadata struct {
	Min  int64
	Max  int64
	Size int
}

// TimeValues is an immutable collection of time values.
type TimeValues interface {
	// Metadata returns the collection metadata.
	Metadata() TimeValuesMetadata

	// Iter returns an iterator to provides iterative access to the underlying dataset
	// when the iterator is created. After the iterator is returned, the iterator has
	// no access to future values added to the underlying dataset. The iterator remains
	// valid until the time values collection is closed.
	// TODO(xichen): Change this to `ForwardTimeIterator`.
	Iter() encoding.RewindableTimeIterator
}

type closeableTimeValues interface {
	TimeValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// timeValuesBuilder incrementally builds the time value collection.
type timeValuesBuilder interface {
	// Add adds a time to the collection.
	Add(v int64) error

	// Snapshot takes a snapshot of the time values collected so far.
	Snapshot() closeableTimeValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable time values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() closeableTimeValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

var (
	errTimeValuesBuilderAlreadyClosed = errors.New("time values builder is already closed")
)

// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type arrayBasedTimeValues struct {
	closed   bool
	initDone bool
	min      int64
	max      int64
	vals     *pool.RefCountedPooledInt64Array
}

func newArrayBasedTimeValues(p *pool.BucketizedInt64ArrayPool) *arrayBasedTimeValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledInt64Array(rawArr, p)
	return &arrayBasedTimeValues{
		vals: refCountedArr,
	}
}

func (b *arrayBasedTimeValues) Metadata() TimeValuesMetadata {
	return TimeValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

func (b *arrayBasedTimeValues) Iter() encoding.RewindableTimeIterator {
	return encoding.NewArrayBasedTimeIterator(b.vals.Get())
}

func (b *arrayBasedTimeValues) Add(v int64) error {
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

func (b *arrayBasedTimeValues) Snapshot() closeableTimeValues {
	return &arrayBasedTimeValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

func (b *arrayBasedTimeValues) Seal() closeableTimeValues {
	sealed := &arrayBasedTimeValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = arrayBasedTimeValues{}
	b.Close()

	return sealed
}

func (b *arrayBasedTimeValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
