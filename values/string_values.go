package values

import (
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
}

// CloseableStringValues is a string values collection that can be closed.
type CloseableStringValues interface {
	StringValues

	// Close closes the value collection. It will also release the resources held for
	// the collection iff there is no one holding references to the collection.
	Close()
}
