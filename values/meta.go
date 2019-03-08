package values

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
)

// MetaUnion is a union of values meta.
type MetaUnion struct {
	Type       field.ValueType
	BoolMeta   BoolValuesMetadata
	IntMeta    IntValuesMetadata
	DoubleMeta DoubleValuesMetadata
	BytesMeta BytesValuesMetadata
	TimeMeta   TimeValuesMetadata
}

// ToMinMaxValueUnion extracts the min value union and max value union from the meta union.
func (u *MetaUnion) ToMinMaxValueUnion() (minUnion, maxUnion field.ValueUnion, err error) {
	minUnion.Type = u.Type
	maxUnion.Type = u.Type
	switch u.Type {
	case field.NullType:
		break
	case field.BoolType:
		if u.BoolMeta.NumTrues > 0 {
			maxUnion.BoolVal = true
			minUnion.BoolVal = true
		}
		if u.BoolMeta.NumFalses > 0 {
			minUnion.BoolVal = false
		}
	case field.IntType:
		minUnion.IntVal = u.IntMeta.Min
		maxUnion.IntVal = u.IntMeta.Max
	case field.DoubleType:
		minUnion.DoubleVal = u.DoubleMeta.Min
		maxUnion.DoubleVal = u.DoubleMeta.Max
	case field.BytesType:
		minUnion.BytesVal = u.BytesMeta.Min
		maxUnion.BytesVal = u.BytesMeta.Max
	case field.TimeType:
		minUnion.TimeNanosVal = u.TimeMeta.Min
		maxUnion.TimeNanosVal = u.TimeMeta.Max
	default:
		return field.ValueUnion{}, field.ValueUnion{}, fmt.Errorf("unknown type %v in meta union", u.Type)
	}
	return minUnion, maxUnion, nil
}

// MustToMinMaxValueUnion extracts the min value union and max value union from the meta union,
// and panics if an error is encountered.
func (u *MetaUnion) MustToMinMaxValueUnion() (minUnion, maxUnion field.ValueUnion) {
	minUnion, maxUnion, err := u.ToMinMaxValueUnion()
	if err != nil {
		panic(err)
	}
	return minUnion, maxUnion
}
