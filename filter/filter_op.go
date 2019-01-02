package filter

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/document/field"
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

// StringFilter returns a string filter using the given value as the filter arguments.
func (f Op) StringFilter(v *field.ValueUnion) (StringFilter, error) {
	if !f.IsValueFilter() {
		return nil, fmt.Errorf("operator %v is not a value filter", f)
	}
	if v == nil {
		return nil, fmt.Errorf("operator %v has nil RHS operand", f)
	}
	switch v.Type {
	case field.StringType:
		filterGen, exists := stringToStringFilterOps[f]
		if !exists {
			return nil, fmt.Errorf("operator %v does not have a string filter generator for string RHS value", f)
		}
		return filterGen(v.StringVal), nil
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
	stringToStringFilterOps = map[Op]stringToStringFilterFn{
		Equals:             equalsStringString,
		NotEquals:          notEqualsStringString,
		LargerThan:         largerThanStringString,
		LargerThanOrEqual:  largerThanOrEqualStringString,
		SmallerThan:        smallerThanStringString,
		SmallerThanOrEqual: smallerThanOrEqualStringString,
		StartsWith:         startsWithStringString,
		DoesNotStartWith:   doesNotStartWithStringString,
		EndsWith:           endsWithStringString,
		DoesNotEndWith:     doesNotEndWithStringString,
		Contains:           containsStringString,
		DoesNotContain:     doesNotContainStringString,
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
			field.StringType: struct{}{},
			field.TimeType:   struct{}{},
		},
		Exists: {
			field.NullType:   struct{}{},
			field.BoolType:   struct{}{},
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.StringType: struct{}{},
			field.TimeType:   struct{}{},
		},
		DoesNotExist: {
			field.NullType:   struct{}{},
			field.BoolType:   struct{}{},
			field.IntType:    struct{}{},
			field.DoubleType: struct{}{},
			field.StringType: struct{}{},
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
	for op := range stringToStringFilterOps {
		addAllowedTypes(op, field.StringType, field.StringType)
	}
	for op := range timeToTimeFilterOps {
		addAllowedTypes(op, field.TimeType, field.TimeType)
	}

	stringToOps = make(map[string]Op, len(opStrings))
	for k, v := range opStrings {
		stringToOps[v] = k
	}
}
