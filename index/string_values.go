package index

import (
	"errors"

	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/x/pool"
)

// StringValuesMetadata contains the metadata for the string values collection.
type StringValuesMetadata struct {
	Min  string
	Max  string
	Size int
}

// StringValues is an immutable collection of string values.
type StringValues interface {
	// Metadata returns the collection metadata.
	Metadata() StringValuesMetadata

	// Iter returns an iterator to provides iterative access to the underlying dataset
	// when the iterator is created. After the iterator is returned, the iterator has
	// no access to future values added to the underlying dataset. The iterator remains
	// valid until the string values collection is closed.
	// TODO(xichen): Change this to `ForwardStringIterator`.
	Iter() encoding.RewindableStringIterator
}

type closeableStringValues interface {
	StringValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// stringValuesBuilder incrementally builds the string value collection.
type stringValuesBuilder interface {
	// Add adds a string to the collection.
	Add(v string) error

	// Snapshot takes a snapshot of the string values collected so far.
	Snapshot() closeableStringValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable string values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() closeableStringValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

var (
	errStringValuesBuilderAlreadyClosed = errors.New("string values builder is already closed")
)

// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type arrayBasedStringValues struct {
	closed   bool
	initDone bool
	min      string
	max      string
	vals     *pool.RefCountedPooledStringArray
}

func newArrayBasedStringValues(p *pool.BucketizedStringArrayPool) *arrayBasedStringValues {
	rawArr := p.Get(defaultInitialFieldValuesCapacity)
	refCountedArr := pool.NewRefCountedPooledStringArray(rawArr, p)
	return &arrayBasedStringValues{
		vals: refCountedArr,
	}
}

func (b *arrayBasedStringValues) Metadata() StringValuesMetadata {
	return StringValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

func (b *arrayBasedStringValues) Iter() encoding.RewindableStringIterator {
	return encoding.NewArrayBasedStringIterator(b.vals.Get())
}

func (b *arrayBasedStringValues) Add(v string) error {
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

func (b *arrayBasedStringValues) Snapshot() closeableStringValues {
	return &arrayBasedStringValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

func (b *arrayBasedStringValues) Seal() closeableStringValues {
	sealed := &arrayBasedStringValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals,
	}

	// Close the current values so it's no longer writable.
	*b = arrayBasedStringValues{}
	b.Close()

	return sealed
}

func (b *arrayBasedStringValues) Close() {
	if b.closed {
		return
	}
	b.closed = true
	if b.vals != nil {
		b.vals.Close()
		b.vals = nil
	}
}
