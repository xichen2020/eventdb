package field

// ValueType is the type of a field value.
type ValueType int

// List of supported field value types.
const (
	Unknown ValueType = iota
	NullType
	BoolType
	IntegerType
	DoubleType
	StringType
)

// Value is a value union.
type Value struct {
	Type      ValueType
	BoolVal   bool
	IntVal    int
	DoubleVal float64
	StringVal string
}

// Field is an event field.
type Field struct {
	Path  []string
	Value Value
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
