package storage

const (
	defaultNestedFieldSeparator = '.'
)

// Options provide a set of options for the database.
type Options struct {
	nestedFieldSeparator byte
}

// NewOptions create a new set of options.
func NewOptions() *Options {
	return &Options{
		nestedFieldSeparator: defaultNestedFieldSeparator,
	}
}

// SetNestedFieldSeparator sets the path separator when flattening nested event fields.
// This is used when persisting and querying nested fields.
func (o *Options) SetNestedFieldSeparator(v byte) *Options {
	opts := *o
	opts.nestedFieldSeparator = v
	return &opts
}

// NestedFieldSeparator returns the path separator when flattening nested event fields.
// This is used when persisting and querying nested fields.
func (o *Options) NestedFieldSeparator() byte {
	return o.nestedFieldSeparator
}
