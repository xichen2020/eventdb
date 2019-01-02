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

func (t ValueType) String() string {
	switch t {
	case NullType:
		return "null"
	case BoolType:
		return "bool"
	case IntType:
		return "int"
	case DoubleType:
		return "double"
	case StringType:
		return "string"
	case TimeType:
		return "time"
	default:
		return "unknown"
	}
}

// ValueTypeSet is a set of value types.
type ValueTypeSet map[ValueType]struct{}

// Clone clones a value type set.
func (m ValueTypeSet) Clone() ValueTypeSet {
	if len(m) == 0 {
		return nil
	}
	cloned := make(ValueTypeSet, len(m))
	for k := range m {
		cloned[k] = struct{}{}
	}
	return cloned
}

var (
	// OrderableTypes is a list of value types eligible for ordering.
	OrderableTypes = ValueTypeSet{
		NullType:   struct{}{},
		BoolType:   struct{}{},
		IntType:    struct{}{},
		DoubleType: struct{}{},
		StringType: struct{}{},
		TimeType:   struct{}{},
	}
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
