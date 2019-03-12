package persist

import (
	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/index/segment"
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

	// Finish marks the persistence as complete.
	Finish() error

	// Close all resources owned by the persist manager.
	Close() error
}

// PrepareOptions provide a set of options for data persistence.
type PrepareOptions struct {
	Namespace   []byte
	SegmentMeta segment.Metadata
}

// Fns contains a set of function that persists document IDs
// and different types of document values for a given field.
type Fns struct {
	WriteFields func(fields []indexfield.DocsField) error
}

// Closer is a function that performs cleanup after persistence.
type Closer func() error

// PreparedPersister is an object that wraps a persist function and a closer.
type PreparedPersister struct {
	Persist Fns
	Close   Closer
}

// RetrieveFieldOptions contains the parameters for retrieving a field.
type RetrieveFieldOptions struct {
	FieldPath  []string
	FieldTypes field.ValueTypeSet
}

// FieldRetriever is responsible for retrieving fields from persistent storage.
// TODO(xichen): Investigate if it's worth providing an async API.
type FieldRetriever interface {
	// RetrieveField retrieves a single field from persistent storage given the
	// field retrieval options. If a field doesn't exist for a given type specified
	// in the options, an error is returned.
	RetrieveField(
		namespace []byte,
		segmentMeta segment.Metadata,
		field RetrieveFieldOptions,
	) (indexfield.DocsField, error)

	// RetrieveFields retrieves a list of fields from persistent storage given the
	// field retrieval options. If a field doesn't exist for a given type specified
	// in the options, an error is returned.
	RetrieveFields(
		namespace []byte,
		segmentMeta segment.Metadata,
		fields []RetrieveFieldOptions,
	) ([]indexfield.DocsField, error)
}
