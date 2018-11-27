package persist

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
// TODO(xichen): Flesh this out.
type PrepareOptions struct {
}

// Fn is a function that persists an in-memory segment.
// TODO(xichen): Flesh this out.
type Fn func() error

// Closer is a function that performs cleanup after persistence.
type Closer func() error

// PreparedPersister is an object that wraps a persist function and a closer.
type PreparedPersister struct {
	Persist Fn
	Close   Closer
}
