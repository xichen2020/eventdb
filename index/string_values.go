package index

import (
	"errors"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/pool"
)

// stringValuesBuilder incrementally builds the string value collection.
type stringValuesBuilder interface {
	// Add adds a string to the collection.
	Add(v string) error

	// Snapshot takes a snapshot of the string values collected so far.
	Snapshot() values.CloseableStringValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable string values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() values.CloseableStringValues

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

func (b *arrayBasedStringValues) Metadata() values.StringValuesMetadata {
	return values.StringValuesMetadata{
		Min:  b.min,
		Max:  b.max,
		Size: len(b.vals.Get()),
	}
}

func (b *arrayBasedStringValues) Iter() (iterator.ForwardStringIterator, error) {
	return iterator.NewArrayBasedStringIterator(b.vals.Get()), nil
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

func (b *arrayBasedStringValues) Snapshot() values.CloseableStringValues {
	return &arrayBasedStringValues{
		min:  b.min,
		max:  b.max,
		vals: b.vals.Snapshot(),
	}
}

func (b *arrayBasedStringValues) Seal() values.CloseableStringValues {
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
