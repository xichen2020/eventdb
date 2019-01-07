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
