package filter

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/x/bytes"
)

// Op represents a filter operator.
type Op int

// A list of supported filter operators.
const (
	UnknownOp Op = iota
	Equals
	NotEquals
	LargerThan
	LargerThanOrEqual
	SmallerThan
	SmallerThanOrEqual
	StartsWith
	DoesNotStartWith
	EndsWith
	DoesNotEndWith
	Contains
	DoesNotContain
	IsNull
	IsNotNull
	Exists
	DoesNotExist
)

// newOp creates a new filter operator.
func newOp(str string) (Op, error) {
	if f, exists := stringToOps[str]; exists {
		return f, nil
	}
	return UnknownOp, fmt.Errorf("unknown filter op string: %s", str)
}

// IsValid returns true if a filter operator is valid.
func (f Op) IsValid() bool {
	_, exists := validFilterOps[f]
	return exists
}

// IsValueFilter returns true if the filter should be applied to individual
// field values.
func (f Op) IsValueFilter() bool {
	_, exists := valueFilterOps[f]
	return exists
}

// IsDocIDSetFilter returns true if the filter should be applied to the doc ID
// set associated with the field values.
func (f Op) IsDocIDSetFilter() bool {
	_, exists := docIDSetFilterOps[f]
	return exists
}

// DocIDSetFilterFn returns the function associated with the filter operator to transform
// an input doc ID set iterator into a new doc ID set iterator, if applicable.
// This is used to distinguish doc ID filter operators such as `Exist` and `DoesNotExist`
// whose application transforms the doc ID set of a field into a new doc ID set. This is in
// contrast with other value filters that operate on the field values.
// If the operator is a valid value filter operator, it returns a nil function with a nil error.
// If the operator is invalid, it returns an error.
func (f Op) DocIDSetFilterFn(numTotalDocs int32) (index.DocIDSetIteratorFn, error) {
	if !f.IsDocIDSetFilter() {
		return nil, fmt.Errorf("operator %v is not a doc ID set filter", f)
	}
	switch f {
	case IsNull, IsNotNull, Exists:
		return index.NoOpDocIDSetIteratorFn, nil
	case DoesNotExist:
		return index.ExcludeDocIDSetIteratorFn(numTotalDocs), nil
	default:
		return nil, fmt.Errorf("unknown doc ID set filter %v", f)
	}
}

// MustDocIDSetFilterFn returns the function associated with the filter operator to transform
// an input doc ID set iterator into a new doc ID set iterator, or panics if an error is
// encountered.
func (f Op) MustDocIDSetFilterFn(numTotalDocs int32) index.DocIDSetIteratorFn {
	fn, err := f.DocIDSetFilterFn(numTotalDocs)
	if err != nil {
		panic(err)
	}
	return fn
}

// BoolFilter returns a bool filter using the given value as the filter arguments.
func (f Op) BoolFilter(v *field.ValueUnion) (BoolFilter, error) {
	if !f.IsValueFilter() {
		return nil, fmt.Errorf("operator %v is not a value filter", f)
	}
	if v == nil {
		return nil, fmt.Errorf("operator %v has nil RHS operand", f)
	}
	switch v.Type {
	case field.BoolType:
		filterGen, exists := boolToBoolFilterOps[f]
		if !exists {
			return nil, fmt.Errorf("operator %v does not have a bool filter generator for bool RHS value", f)
		}
		return filterGen(v.BoolVal), nil
	default:
		return nil, fmt.Errorf("operator %v has an invalid RHS operand type %v", f, v.Type)
	}
}

// IntFilter returns an int filter using the given value as the filter arguments.
func (f Op) IntFilter(v *field.ValueUnion) (IntFilter, error) {
	if !f.IsValueFilter() {
		return nil, fmt.Errorf("operator %v is not a value filter", f)
	}
	if v == nil {
		return nil, fmt.Errorf("operator %v has nil RHS operand", f)
	}
	switch v.Type {
	case field.IntType:
		filterGen, exists := intToIntFilterOps[f]
		if !exists {
			return nil, fmt.Errorf("operator %v does not have an int filter generator for int RHS value", f)
		}
		return filterGen(v.IntVal), nil
	case field.DoubleType:
		filterGen, exists := doubleToIntFilterOps[f]
		if !exists {
			return nil, fmt.Errorf("operator %v does not have an int filter generator for double RHS value", f)
		}
		return filterGen(v.DoubleVal), nil
	default:
		return nil, fmt.Errorf("operator %v has an invalid RHS operand type %v", f, v.Type)
	}
}

// DoubleFilter returns a double filter using the given value as the filter arguments.
func (f Op) DoubleFilter(v *field.ValueUnion) (DoubleFilter, error) {
	if !f.IsValueFilter() {
		return nil, fmt.Errorf("operator %v is not a value filter", f)
	}
	if v == nil {
		return nil, fmt.Errorf("operator %v has nil RHS operand", f)
	}
	switch v.Type {
	case field.IntType:
		filterGen, exists := intToDoubleFilterOps[f]
		if !exists {
			return nil, fmt.Errorf("operator %v does not have a double filter generator for int RHS value", f)
		}
		return filterGen(v.IntVal), nil
	case field.DoubleType:
		filterGen, exists := doubleToDoubleFilterOps[f]
		if !exists {
			return nil, fmt.Errorf("operator %v does not have a double filter generator for double RHS value", f)
		}
		return filterGen(v.DoubleVal), nil
	default:
		return nil, fmt.Errorf("operator %v has an invalid RHS operand type %v", f, v.Type)
	}
}

// BytesFilter returns a bytes filter using the given value as the filter arguments.
func (f Op) BytesFilter(v *field.ValueUnion) (BytesFilter, error) {
	if !f.IsValueFilter() {
		return nil, fmt.Errorf("operator %v is not a value filter", f)
	}
	if v == nil {
		return nil, fmt.Errorf("operator %v has nil RHS operand", f)
	}
	switch v.Type {
	case field.BytesType:
		filterGen, exists := bytesToBytesFilterOps[f]
		if !exists {
			return nil, fmt.Errorf("operator %v does not have a bytes filter generator for bytes RHS value", f)
		}
		return filterGen(v.BytesVal.SafeBytes()), nil
	default:
		return nil, fmt.Errorf("operator %v has an invalid RHS operand type %v", f, v.Type)
	}
}

// TimeFilter returns a time filter using the given value as the filter arguments.
func (f Op) TimeFilter(v *field.ValueUnion) (TimeFilter, error) {
	if !f.IsValueFilter() {
		return nil, fmt.Errorf("operator %v is not a value filter", f)
	}
	if v == nil {
		return nil, fmt.Errorf("operator %v has nil RHS operand", f)
	}
	switch v.Type {
	case field.TimeType:
		filterGen, exists := timeToTimeFilterOps[f]
		if !exists {
			return nil, fmt.Errorf("operator %v does not have a time filter generator for time RHS value", f)
		}
		return filterGen(v.TimeNanosVal), nil
	default:
		return nil, fmt.Errorf("operator %v has an invalid RHS operand type %v", f, v.Type)
	}
}

// AllowedTypes returns a list of value types that are allowed
// for the given filter operator and the right-hand-side value.
func (f Op) AllowedTypes(v *field.ValueUnion) (field.ValueTypeSet, error) {
	if !f.IsValid() {
		return nil, fmt.Errorf("invalid operator %v", f)
	}

	if f.IsDocIDSetFilter() {
		allowed, exists := allowedTypesByDocIDSetFilterOp[f]
		if !exists {
			return nil, fmt.Errorf("doc ID set filter op %v does not have allowed types", f)
		}
		return allowed.Clone(), nil
	}

	// Value filter.
	if v == nil {
		return nil, fmt.Errorf("value filter op %v has a nil RHS operand", f)
	}
	allowedByRHSType, exists := allowedTypesByValueFilterOpAndRHSType[f]
	if !exists {
		return nil, fmt.Errorf("value filter op %v does not have allowed types", f)
	}
	allowed := allowedByRHSType[v.Type]
	return allowed.Clone(), nil
}

// MultiTypeCombinator returns the filter combinator to join the filter result for
// multi-typed field values. Specifically, if a field has values of type A and values
// of type B, applying the operator against the field will produce a result as follows:
// `filter(field) = filter(field_type_A) Combinator filter(field_type_B)`, where the
// Combinator is returned by this function.
func (f Op) MultiTypeCombinator() (Combinator, error) {
	if !f.IsValid() {
		return UnknownCombinator, fmt.Errorf("invalid operator %v", f)
	}
	switch f {
	case DoesNotExist:
		return Or, nil
	default:
		return And, nil
	}
}

// String returns the string representation of the filter operator.
func (f Op) String() string {
	if s, exists := opStrings[f]; exists {
		return s
	}
	// nolint: goconst
	return "unknown"
}

// UnmarshalJSON unmarshals a JSON object as a filter operator.
func (f *Op) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	op, err := newOp(str)
	if err != nil {
		return err
	}
	*f = op
	return nil
}

// ToProto converts a filter op to a filter op proto message.
func (f Op) ToProto() (servicepb.Filter_Op, error) {
	switch f {
	case Equals:
		return servicepb.Filter_EQUALS, nil
	case NotEquals:
		return servicepb.Filter_NOTEQUALS, nil
	case LargerThan:
		return servicepb.Filter_LARGERTHAN, nil
	case LargerThanOrEqual:
		return servicepb.Filter_LARGERTHANOREQUAL, nil
	case SmallerThan:
		return servicepb.Filter_SMALLERTHAN, nil
	case SmallerThanOrEqual:
		return servicepb.Filter_SMALLERTHANOREQUAL, nil
	case StartsWith:
		return servicepb.Filter_STARTSWITH, nil
	case DoesNotStartWith:
		return servicepb.Filter_DOESNOTSTARTWITH, nil
	case EndsWith:
		return servicepb.Filter_ENDSWITH, nil
	case DoesNotEndWith:
		return servicepb.Filter_DOESNOTENDWITH, nil
	case Contains:
		return servicepb.Filter_CONTAINS, nil
	case DoesNotContain:
		return servicepb.Filter_DOESNOTCONTAIN, nil
	case IsNull:
		return servicepb.Filter_ISNULL, nil
	case IsNotNull:
		return servicepb.Filter_ISNOTNULL, nil
	case Exists:
		return servicepb.Filter_EXISTS, nil
	case DoesNotExist:
		return servicepb.Filter_DOESNOTEXIST, nil
	default:
		return servicepb.Filter_UNKNOWNOP, fmt.Errorf("invalid protobuf filter op %v", f)
	}
}

// IntMaybeInRange returns true if filterVal is within the range defined by min and max.
// If this returns false, it means filterVal is definitely not within the value range [min, max].
// If this returns true, it doesn't necessarily mean the filterVal exists in the values that this
// filter is acting on.
func (f Op) IntMaybeInRange(min, max, filterVal int) bool {
	switch f {
	case Equals:
		if filterVal > max || filterVal < min {
			return false
		}
	case LargerThan:
		if filterVal >= max {
			return false
		}
	case LargerThanOrEqual:
		if filterVal > max {
			return false
		}
	case SmallerThan:
		if filterVal <= min {
			return false
		}
	case SmallerThanOrEqual:
		if filterVal < min {
			return false
		}
	}
	return true
}

// BytesMaybeInRange returns true if filterVal is within the range defined in by min and max.
// If this returns false, it means filterVal is definitely not within the value range [min, max].
// If this returns true, it doesn't necessarily mean the filterVal exists in the values that this
// filter is acting on.
func (f Op) BytesMaybeInRange(min, max, filterVal []byte) bool {
	switch f {
	case Equals, StartsWith:
		if bytes.GreaterThan(filterVal, max) || bytes.LessThan(filterVal, min) {
			return false
		}
	case LargerThan:
		if bytes.GreaterThanOrEqual(filterVal, max) {
			return false
		}
	case LargerThanOrEqual:
		if bytes.GreaterThan(filterVal, max) {
			return false
		}
	case SmallerThan:
		if bytes.LessThanOrEqual(filterVal, min) {
			return false
		}
	case SmallerThanOrEqual:
		if bytes.LessThan(filterVal, min) {
			return false
		}
	}
	return true
}

// BoolIsInRange returns true if filterVal exists in the values that this filter is acting on.
func (f Op) BoolIsInRange(numTrues, numFalses int, filterVal bool) bool {
	switch f {
	case Equals:
		if filterVal && numTrues == 0 {
			return false
		}
		if !filterVal && numFalses == 0 {
			return false
		}
	case NotEquals:
		if filterVal && numFalses == 0 {
			return false
		}
		if !filterVal && numTrues == 0 {
			return false
		}
	}
	return true
}

// DoubleMaybeInRange returns true if filterVal is within the range defined by min and max.
// If this returns false, it means filterVal is definitely not within the value range [min, max].
// If this returns true, it doesn't necessarily mean the filterVal exists in the values that this
// filter is acting on.
func (f Op) DoubleMaybeInRange(min, max, filterVal float64) bool {
	switch f {
	case Equals:
		if filterVal > max || filterVal < min {
			return false
		}
	case LargerThan:
		if filterVal >= max {
			return false
		}
	case LargerThanOrEqual:
		if filterVal > max {
			return false
		}
	case SmallerThan:
		if filterVal <= min {
			return false
		}
	case SmallerThanOrEqual:
		if filterVal < min {
			return false
		}
	}
	return true
}

// TimeMaybeInRange returns true if filterVal is within the range defined by min and max.
// If this returns false, it means filterVal is definitely not within the value range [min, max].
// If this returns true, it doesn't necessarily mean the filterVal exists in the values that this
// filter is acting on.
func (f Op) TimeMaybeInRange(min, max, filterVal int64) bool {
	switch f {
	case Equals:
		if filterVal > max || filterVal < min {
			return false
		}
	case LargerThan:
		if filterVal >= max {
			return false
		}
	case LargerThanOrEqual:
		if filterVal > max {
			return false
		}
	case SmallerThan:
		if filterVal <= min {
			return false
		}
	case SmallerThanOrEqual:
		if filterVal < min {
			return false
		}
	}
	return true
}

var (
	valueFilterOps = map[Op]struct{}{
		Equals:             struct{}{},
		NotEquals:          struct{}{},
		LargerThan:         struct{}{},
		LargerThanOrEqual:  struct{}{},
		SmallerThan:        struct{}{},
		SmallerThanOrEqual: struct{}{},
		StartsWith:         struct{}{},
		DoesNotStartWith:   struct{}{},
		EndsWith:           struct{}{},
		DoesNotEndWith:     struct{}{},
		Contains:           struct{}{},
		DoesNotContain:     struct{}{},
	}
	docIDSetFilterOps = map[Op]struct{}{
		IsNull:       struct{}{},
		IsNotNull:    struct{}{},
		Exists:       struct{}{},
		DoesNotExist: struct{}{},
	}
	validFilterOps map[Op]struct{}

	boolToBoolFilterOps = map[Op]boolToBoolFilterFn{
		Equals:    equalsBoolBool,
		NotEquals: notEqualsBoolBool,
	}
	intToIntFilterOps = map[Op]intToIntFilterFn{
		Equals:             equalsIntInt,
		NotEquals:          notEqualsIntInt,
		LargerThan:         largerThanIntInt,
		LargerThanOrEqual:  largerThanOrEqualIntInt,
		SmallerThan:        smallerThanIntInt,
		SmallerThanOrEqual: smallerThanOrEqualIntInt,
	}
	doubleToIntFilterOps = map[Op]doubleToIntFilterFn{
		Equals:             equalsIntDouble,
		NotEquals:          notEqualsIntDouble,
		LargerThan:         largerThanIntDouble,
		LargerThanOrEqual:  largerThanOrEqualIntDouble,
		SmallerThan:        smallerThanIntDouble,
		SmallerThanOrEqual: smallerThanOrEqualIntDouble,
	}
	doubleToDoubleFilterOps = map[Op]doubleToDoubleFilterFn{
		Equals:             equalsDoubleDouble,
		NotEquals:          notEqualsDoubleDouble,
		LargerThan:         largerThanDoubleDouble,
		LargerThanOrEqual:  largerThanOrEqualDoubleDouble,
		SmallerThan:        smallerThanDoubleDouble,
		SmallerThanOrEqual: smallerThanOrEqualDoubleDouble,
	}
	intToDoubleFilterOps = map[Op]intToDoubleFilterFn{
		Equals:             equalsDoubleInt,
		NotEquals:          notEqualsDoubleInt,
		LargerThan:         largerThanDoubleInt,
		LargerThanOrEqual:  largerThanOrEqualDoubleInt,
		SmallerThan:        smallerThanDoubleInt,
		SmallerThanOrEqual: smallerThanOrEqualDoubleInt,
	}
	bytesToBytesFilterOps = map[Op]bytesToBytesFilterFn{
		Equals:             equalsBytesBytes,
		NotEquals:          notEqualsBytesBytes,
		LargerThan:         largerThanBytesBytes,
		LargerThanOrEqual:  largerThanOrEqualBytesBytes,
		SmallerThan:        smallerThanBytesBytes,
		SmallerThanOrEqual: smallerThanOrEqualBytesBytes,
		StartsWith:         startsWithBytesBytes,
		DoesNotStartWith:   doesNotStartWithBytesBytes,
		EndsWith:           endsWithBytesBytes,
		DoesNotEndWith:     doesNotEndWithBytesBytes,
		Contains:           containsBytesBytes,
		DoesNotContain:     doesNotContainBytesBytes,
	}
	timeToTimeFilterOps = map[Op]timeToTimeFilterFn{
		Equals:             equalsTimeTime,
		NotEquals:          notEqualsTimeTime,
		LargerThan:         largerThanTimeTime,
		LargerThanOrEqual:  largerThanOrEqualTimeTime,
		SmallerThan:        smallerThanTimeTime,
		SmallerThanOrEqual: smallerThanOrEqualTimeTime,
	}

	allowedTypesByValueFilterOpAndRHSType map[Op]map[field.ValueType]field.ValueTypeSet
	allowedTypesByDocIDSetFilterOp        = map[Op]field.ValueTypeSet{
		IsNull: {
			field.NullType: struct{}{},
		},
		IsNotNull: {
			field.BoolType:   struct{}{},
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.BytesType:  struct{}{},
			field.TimeType:   struct{}{},
		},
		Exists: {
			field.NullType:   struct{}{},
			field.BoolType:   struct{}{},
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.BytesType:  struct{}{},
			field.TimeType:   struct{}{},
		},
		DoesNotExist: {
			field.NullType:   struct{}{},
			field.BoolType:   struct{}{},
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.BytesType:  struct{}{},
			field.TimeType:   struct{}{},
		},
	}

	opStrings = map[Op]string{
		Equals:             "=",
		NotEquals:          "!=",
		LargerThan:         ">",
		LargerThanOrEqual:  ">=",
		SmallerThan:        "<",
		SmallerThanOrEqual: "<=",
		StartsWith:         "startsWith",
		DoesNotStartWith:   "notStartsWith",
		EndsWith:           "endsWith",
		DoesNotEndWith:     "notEndsWith",
		Contains:           "contains",
		DoesNotContain:     "notContains",
		IsNull:             "isNull",
		IsNotNull:          "isNotNull",
		Exists:             "exists",
		DoesNotExist:       "notExists",
	}
	stringToOps map[string]Op
)

func addAllowedTypes(op Op, lhsType, rhsType field.ValueType) {
	byRHSTypeMap, exists := allowedTypesByValueFilterOpAndRHSType[op]
	if !exists {
		byRHSTypeMap = make(map[field.ValueType]field.ValueTypeSet)
		allowedTypesByValueFilterOpAndRHSType[op] = byRHSTypeMap
	}
	valueTypes, exists := byRHSTypeMap[rhsType]
	if !exists {
		valueTypes = make(field.ValueTypeSet)
		byRHSTypeMap[rhsType] = valueTypes
	}
	valueTypes[lhsType] = struct{}{}
}

func init() {
	validFilterOps = make(map[Op]struct{}, len(valueFilterOps)+len(docIDSetFilterOps))
	for vo := range valueFilterOps {
		validFilterOps[vo] = struct{}{}
	}
	for do := range docIDSetFilterOps {
		validFilterOps[do] = struct{}{}
	}

	size := len(valueFilterOps)
	allowedTypesByValueFilterOpAndRHSType = make(map[Op]map[field.ValueType]field.ValueTypeSet, size)
	for op := range boolToBoolFilterOps {
		addAllowedTypes(op, field.BoolType, field.BoolType)
	}
	for op := range intToIntFilterOps {
		addAllowedTypes(op, field.IntType, field.IntType)
	}
	for op := range doubleToIntFilterOps {
		addAllowedTypes(op, field.IntType, field.DoubleType)
	}
	for op := range doubleToDoubleFilterOps {
		addAllowedTypes(op, field.DoubleType, field.DoubleType)
	}
	for op := range intToDoubleFilterOps {
		addAllowedTypes(op, field.DoubleType, field.IntType)
	}
	for op := range bytesToBytesFilterOps {
		addAllowedTypes(op, field.BytesType, field.BytesType)
	}
	for op := range timeToTimeFilterOps {
		addAllowedTypes(op, field.TimeType, field.TimeType)
	}

	stringToOps = make(map[string]Op, len(opStrings))
	for k, v := range opStrings {
		stringToOps[v] = k
	}
}
