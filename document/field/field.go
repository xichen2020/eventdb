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

// IsValid returns true if this is a valid value type.
func (t ValueType) IsValid() bool {
	_, exists := validTypes[t]
	return exists
}

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
	// validTypes is a list of valid value types.
	validTypes = ValueTypeSet{
		NullType:   struct{}{},
		BoolType:   struct{}{},
		IntType:    struct{}{},
		DoubleType: struct{}{},
		StringType: struct{}{},
		TimeType:   struct{}{},
	}

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

var (
	// NullUnion is a null value union.
	NullUnion = ValueUnion{Type: NullType}
)

// BoolAsUnionFn converts a bool to a value union.
type BoolAsUnionFn func(v bool) ValueUnion

// IntAsUnionFn converts an int to a value union.
type IntAsUnionFn func(v int) ValueUnion

// DoubleAsUnionFn converts a double to a value union.
type DoubleAsUnionFn func(v float64) ValueUnion

// StringAsUnionFn converts a string to a value union.
type StringAsUnionFn func(v string) ValueUnion

// TimeAsUnionFn converts a time to a value union.
type TimeAsUnionFn func(v int64) ValueUnion

// NewBoolUnion creates a new bool union.
func NewBoolUnion(v bool) ValueUnion {
	return ValueUnion{
		Type:    BoolType,
		BoolVal: v,
	}
}

// NewIntUnion creates a new int union.
func NewIntUnion(v int) ValueUnion {
	return ValueUnion{
		Type:   IntType,
		IntVal: v,
	}
}

// NewDoubleUnion creates a new double union.
func NewDoubleUnion(v float64) ValueUnion {
	return ValueUnion{
		Type:      DoubleType,
		DoubleVal: v,
	}
}

// NewStringUnion creates a new string union.
func NewStringUnion(v string) ValueUnion {
	return ValueUnion{
		Type:      StringType,
		StringVal: v,
	}
}

// NewTimeUnion creates a new time union.
func NewTimeUnion(v int64) ValueUnion {
	return ValueUnion{
		Type:         TimeType,
		TimeNanosVal: v,
	}
}

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
