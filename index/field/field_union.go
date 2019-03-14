package field

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/values"
)

// Union is a union of different typed fields.
type Union struct {
	Type        field.ValueType
	NullField   NullField
	BoolField   BoolField
	IntField    IntField
	DoubleField DoubleField
	BytesField BytesField
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
	case field.BytesType:
		return u.BytesField.Iter()
	case field.TimeType:
		return u.TimeField.Iter()
	}
	return nil, fmt.Errorf("unknown field type in union: %v", u.Type)
}

// ValuesMeta returns the corresponding values metadata for the field represented
// by the union, or an error otherwise.
func (u *Union) ValuesMeta() (values.MetaUnion, error) {
	mu := values.MetaUnion{Type: u.Type}
	switch u.Type {
	case field.NullType:
		break
	case field.BoolType:
		mu.BoolMeta = u.BoolField.Values().Metadata()
	case field.IntType:
		mu.IntMeta = u.IntField.Values().Metadata()
	case field.DoubleType:
		mu.DoubleMeta = u.DoubleField.Values().Metadata()
	case field.BytesType:
		mu.BytesMeta = u.BytesField.Values().Metadata()
	case field.TimeType:
		mu.TimeMeta = u.TimeField.Values().Metadata()
	default:
		return values.MetaUnion{}, fmt.Errorf("unknown field type in union: %v", u.Type)
	}
	return mu, nil
}

// MustValuesMeta returns the values metadata for the field represented by the union,
// and panics if an error is encountered.
func (u *Union) MustValuesMeta() values.MetaUnion {
	mu, err := u.ValuesMeta()
	if err != nil {
		panic(err)
	}
	return mu
}
