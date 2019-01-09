package values

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values/iterator"
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
	Iter() (iterator.ForwardTimeIterator, error)

	// Filter applies the given filter against the values, returning an iterator
	// identifying the positions of values matching the filter.
	Filter(op filter.Op, filterValue *field.ValueUnion) (iterator.PositionIterator, error)
}

// CloseableTimeValues is a collection of time values that can be closed.
type CloseableTimeValues interface {
	TimeValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// TimeValuesBuilder incrementally builds the time value collection.
type TimeValuesBuilder interface {
	// Add adds a time to the collection.
	Add(v int64) error

	// Snapshot takes a snapshot of the time values collected so far.
	Snapshot() CloseableTimeValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable time values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() CloseableTimeValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
