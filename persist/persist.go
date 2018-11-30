package persist

import "github.com/pilosa/pilosa/roaring"

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
	MinTimeNanos int64
	MaxTimeNanos int64
	NumDocuments int32
}

// Fns contains a set of function that persists document IDs
// and different types of document values for a given field.
type Fns struct {
	WriteNullField   func(fieldPath []string, docIDs *roaring.Bitmap) error
	WriteBoolField   func(fieldPath []string, docIDs *roaring.Bitmap, vals []bool) error
	WriteIntField    func(fieldPath []string, docIDs *roaring.Bitmap, vals []int) error
	WriteDoubleField func(fieldPath []string, docIDs *roaring.Bitmap, vals []float64) error
	WriteStringField func(fieldPath []string, docIDs *roaring.Bitmap, vals []string) error
}

// Closer is a function that performs cleanup after persistence.
type Closer func() error

// PreparedPersister is an object that wraps a persist function and a closer.
type PreparedPersister struct {
	Persist Fns
	Close   Closer
}
