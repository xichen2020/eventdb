package values

import (
	"github.com/xichen2020/eventdb/values/iterator"
)

// DoubleValuesMetadata contains the metadata for the double values collection.
type DoubleValuesMetadata struct {
	Min  float64
	Max  float64
	Size int
}

// DoubleValues is an immutable collection of double values.
type DoubleValues interface {
	// Metadata returns the collection metadata.
	Metadata() DoubleValuesMetadata

	// Iter returns an iterator to provides iterative access to the underlying dataset
	// when the iterator is created. After the iterator is returned, the iterator has
	// no access to future values added to the underlying dataset. The iterator remains
	// valid until the double values collection is closed.
	Iter() (iterator.ForwardDoubleIterator, error)
}

// CloseableDoubleValues is a collection of double values that can be closed.
type CloseableDoubleValues interface {
	DoubleValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
