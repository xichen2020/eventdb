package storage

// Options to configure a storage instance.
type Options interface{}

type options struct{}

// NewOptions creates a new set of storage options.
func NewOptions() Options {
	return &options{}
}
