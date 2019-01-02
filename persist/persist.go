package persist

import (
	"github.com/xichen2020/eventdb/index"
)

// Manager manages the internals of persisting data onto storage layer.
type Manager interface {
	// StartPersist starts persisting data.
	StartPersist() (Persister, error)
}

// Persister is responsible for actually persisting data.
type Persister interface {
	// Prepare prepares for data persistence.
	Prepare(opts PrepareOptions) (PreparedPersister, error)

	// Done marks the persistence as complete.
	Done() error
}

// PrepareOptions provide a set of options for data persistence.
type PrepareOptions struct {
	Namespace    []byte
	Shard        uint32
	SegmentID    string
	MinTimeNanos int64
	MaxTimeNanos int64
	NumDocuments int32
}

// Fns contains a set of function that persists document IDs
// and different types of document values for a given field.
type Fns struct {
	WriteFields func(fields []index.DocsField) error
}

// Closer is a function that performs cleanup after persistence.
type Closer func() error

// PreparedPersister is an object that wraps a persist function and a closer.
type PreparedPersister struct {
	Persist Fns
	Close   Closer
}
