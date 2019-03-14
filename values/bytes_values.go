package values

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values/iterator"
)

// BytesValuesMetadata contains the metadata for the bytes values collection.
type BytesValuesMetadata struct {
	Min  []byte
	Max  []byte
	Size int
}

// BytesValues is an immutable collection of bytes values.
type BytesValues interface {
	// Metadata returns the collection metadata.
	Metadata() BytesValuesMetadata

	// Iter returns an iterator to provides iterative access to the underlying dataset
	// when the iterator is created. After the iterator is returned, the iterator has
	// no access to future values added to the underlying dataset. The iterator remains
	// valid until the bytes values collection is closed.
	Iter() (iterator.ForwardBytesIterator, error)

	// Filter applies the given filter against the values, returning an iterator
	// identifying the positions of values matching the filter.
	Filter(op filter.Op, filterValue *field.ValueUnion) (iterator.PositionIterator, error)
}

// CloseableBytesValues is a bytes values collection that can be closed.
type CloseableBytesValues interface {
	BytesValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// BytesValuesBuilder incrementally builds the bytes value collection.
type BytesValuesBuilder interface {
	// Add adds a bytes to the collection.
	Add(v []byte) error

	// Snapshot takes a snapshot of the bytes values collected so far.
	Snapshot() CloseableBytesValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable bytes values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() CloseableBytesValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
