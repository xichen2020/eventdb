package values

import (
	"github.com/xichen2020/eventdb/values/iterator"
)

// BoolValuesMetadata contains the metadata for the bool values collection.
type BoolValuesMetadata struct {
	NumTrues  int
	NumFalses int
}

// BoolValues is an immutable collection of bool values.
type BoolValues interface {
	// Metadata returns the collection metadata.
	Metadata() BoolValuesMetadata

	// Iter returns an iterator to provides iterative access to the underlying dataset
	// when the iterator is created. After the iterator is returned, the iterator has
	// no access to future values added to the underlying dataset. The iterator remains
	// valid until the bool values collection is closed.
	Iter() (iterator.ForwardBoolIterator, error)
}

// CloseableBoolValues is a collection of bool values that can be closed.
type CloseableBoolValues interface {
	BoolValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}

// BoolValuesBuilder incrementally builds the bool value collection.
type BoolValuesBuilder interface {
	// Add adds a bool to the collection.
	Add(v bool) error

	// Snapshot takes a snapshot of the bool values collected so far.
	Snapshot() CloseableBoolValues

	// Seal seals and closes the mutable collection, and returns an
	// immutable bool values collection. The resource ownership is
	// transferred from the builder to the immutable collection as a result.
	// Adding more data to the builder after the builder is sealed will result
	// in an error.
	Seal() CloseableBoolValues

	// Close closes the builder. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
