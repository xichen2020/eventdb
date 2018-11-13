package value

// Type is a value type.
type Type int

// A list of supported value types.
const (
	UnknownType Type = iota
	NullType
	BoolType
	StringType
	NumberType
	ArrayType
	ObjectType
)

// String returns string representation of t.
func (t Type) String() string {
	switch t {
	case NullType:
		return "null"
	case BoolType:
		return "bool"
	case StringType:
		return "string"
	case NumberType:
		return "number"
	case ArrayType:
		return "array"
	case ObjectType:
		return "object"
	default:
		return "unknown"
	}
}
