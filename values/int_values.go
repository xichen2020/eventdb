package values

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values/iterator"
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
	Iter() (iterator.ForwardIntIterator, error)

	// Filter applies the given filter against the values, returning an iterator
	// identifying the positions of values matching the filter.
	Filter(op filter.Op, filterValue *field.ValueUnion) (iterator.PositionIterator, error)
}

// CloseableIntValues is a collection of int values that can be closed.
type CloseableIntValues interface {
	IntValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// IntValuesBuilder incrementally builds the int value collection.
type IntValuesBuilder interface {
	// Add adds a int to the collection.
	Add(v int) error

	// Snapshot takes a snapshot of the int values collected so far.
	Snapshot() CloseableIntValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable int values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() CloseableIntValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
