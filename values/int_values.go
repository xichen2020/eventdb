package values

import (
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
}

// CloseableIntValues is a collection of int values that can be closed.
type CloseableIntValues interface {
	IntValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
