package field

// Field is an event field.
type Field struct {
	Path  []string
	Value ValueUnion
}

// Iterator iterate over a set of fields.
type Iterator interface {
	// Next returns true if there are more fields to be iterated over,
	// and false otherwise.
	Next() bool

	// Current returns the current field. The field remains valid
	// until the next Next() call.
	Current() Field

	// Close closes the iterator.
	Close()
}
