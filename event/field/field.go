package field

// ValueType is the type of a field value.
type ValueType int

// List of supported field value types.
const (
	Unknown ValueType = iota
	NullType
	BoolType
	IntType
	DoubleType
	StringType
	TimeType
)

// ValueUnion is a value union.
type ValueUnion struct {
	Type         ValueType
	BoolVal      bool
	IntVal       int
	DoubleVal    float64
	StringVal    string
	TimeNanosVal int64
}

// Field is an event field.
type Field struct {
	Path  []string
	Value ValueUnion
}

// Iterator iterate over a set of fields
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
