package values

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values/iterator"
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
	Iter() (iterator.ForwardStringIterator, error)

	// Filter applies the given filter against the values, returning an iterator
	// identifying the positions of values matching the filter.
	Filter(op filter.Op, filterValue *field.ValueUnion) (iterator.PositionIterator, error)
}

// CloseableStringValues is a string values collection that can be closed.
type CloseableStringValues interface {
	StringValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// StringValuesBuilder incrementally builds the string value collection.
type StringValuesBuilder interface {
	// Add adds a string to the collection.
	Add(v string) error

	// Snapshot takes a snapshot of the string values collected so far.
	Snapshot() CloseableStringValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable string values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() CloseableStringValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
