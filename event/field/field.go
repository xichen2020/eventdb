package field

import (
	"fmt"
)

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

var (
	comparableTypeMap = map[ValueType][]ValueType{
		NullType:   nil,
		BoolType:   []ValueType{BoolType},
		IntType:    []ValueType{IntType, DoubleType},
		DoubleType: []ValueType{IntType, DoubleType},
		StringType: []ValueType{StringType},
		TimeType:   []ValueType{TimeType},
	}
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

// ComparableTypes returns a list of value types such that a value of the
// current type `t` can be compared against values of any value type in the list.
func (t ValueType) ComparableTypes() ([]ValueType, error) {
	vts, exists := comparableTypeMap[t]
	if exists {
		return vts, nil
	}
	return nil, fmt.Errorf("unknown value type %v does not have comparable types", t)
}

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
