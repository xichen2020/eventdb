package values

import (
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
}

// CloseableTimeValues is a collection of time values that can be closed.
type CloseableTimeValues interface {
	TimeValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
