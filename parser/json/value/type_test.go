package value

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueType(t *testing.T) {
	inputs := []struct {
		vt       Type
		expected string
	}{
		{vt: NullType, expected: "null"},
		{vt: BoolType, expected: "bool"},
		{vt: BytesType, expected: "string"},
		{vt: NumberType, expected: "number"},
		{vt: ArrayType, expected: "array"},
		{vt: ObjectType, expected: "object"},
		{vt: UnknownType, expected: "unknown"},
		{vt: Type(100), expected: "unknown"},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.vt.String())
	}
}
