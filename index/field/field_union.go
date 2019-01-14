package field

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
)

// Union is a union of different typed fields.
type Union struct {
	Type        field.ValueType
	NullField   NullField
	BoolField   BoolField
	IntField    IntField
	DoubleField DoubleField
	StringField StringField
	TimeField   TimeField
}

// Iter returns the corresponding iterator for the field represented by the union,
// or an error otherwise.
func (u *Union) Iter() (BaseFieldIterator, error) {
	switch u.Type {
	case field.NullType:
		return u.NullField.Iter(), nil
	case field.BoolType:
		return u.BoolField.Iter()
	case field.IntType:
		return u.IntField.Iter()
	case field.DoubleType:
		return u.DoubleField.Iter()
	case field.StringType:
		return u.StringField.Iter()
	case field.TimeType:
		return u.TimeField.Iter()
	}
	return nil, fmt.Errorf("unknown field type in union: %v", u.Type)
}
