package query

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/x/compare"
)

// SortOrder represents a sort order.
type SortOrder int

// A list of supported sort orders.
const (
	UnknownSortOrder SortOrder = iota
	Ascending
	Descending
)

func newSortOrder(str string) (SortOrder, error) {
	if f, exists := stringToSortOrders[str]; exists {
		return f, nil
	}
	return UnknownSortOrder, fmt.Errorf("unknown sort order string: %s", str)
}

// CompareBoolFn compares two boolean values.
func (f SortOrder) CompareBoolFn() (compare.BoolCompareFn, error) {
	switch f {
	case Ascending:
		return compare.BoolCompare, nil
	case Descending:
		return compare.ReverseBoolCompare, nil
	default:
		return nil, fmt.Errorf("unknown sort order %v", f)
	}
}

// CompareIntFn compares two int values.
func (f SortOrder) CompareIntFn() (compare.IntCompareFn, error) {
	switch f {
	case Ascending:
		return compare.IntCompare, nil
	case Descending:
		return compare.ReverseIntCompare, nil
	default:
		return nil, fmt.Errorf("unknown sort order %v", f)
	}
}

// CompareDoubleFn compares two double values.
func (f SortOrder) CompareDoubleFn() (compare.DoubleCompareFn, error) {
	switch f {
	case Ascending:
		return compare.DoubleCompare, nil
	case Descending:
		return compare.ReverseDoubleCompare, nil
	default:
		return nil, fmt.Errorf("unknown sort order %v", f)
	}
}

// CompareBytesFn compares two string values.
func (f SortOrder) CompareBytesFn() (compare.BytesCompareFn, error) {
	switch f {
	case Ascending:
		return compare.BytesCompare, nil
	case Descending:
		return compare.ReverseBytesCompare, nil
	default:
		return nil, fmt.Errorf("unknown sort order %v", f)
	}
}

// CompareTimeFn compares two time values.
func (f SortOrder) CompareTimeFn() (compare.TimeCompareFn, error) {
	switch f {
	case Ascending:
		return compare.TimeCompare, nil
	case Descending:
		return compare.ReverseTimeCompare, nil
	default:
		return nil, fmt.Errorf("unknown sort order %v", f)
	}
}

// CompareFieldValueFn returns the function to compare two field values.
func (f SortOrder) CompareFieldValueFn() (field.ValueCompareFn, error) {
	switch f {
	case Ascending:
		return field.MustCompareValue, nil
	case Descending:
		return field.MustReverseCompareValue, nil
	default:
		return nil, fmt.Errorf("unknown sort order %v", f)
	}
}

// CompareFieldValue compares two field values.
func (f SortOrder) CompareFieldValue(v1, v2 field.ValueUnion) (int, error) {
	switch f {
	case Ascending:
		return field.CompareValue(v1, v2)
	case Descending:
		return field.CompareValue(v2, v1)
	default:
		return 0, fmt.Errorf("unknown sort order %v", f)
	}
}

// MustCompareFieldValue compares two field values, or panics if an error is encountered.
func (f SortOrder) MustCompareFieldValue(v1, v2 field.ValueUnion) int {
	res, err := f.CompareFieldValue(v1, v2)
	if err != nil {
		panic(err)
	}
	return res
}

// CompareCalcValueFn returns the function to compare two calculation values.
func (f SortOrder) CompareCalcValueFn() (calculation.ValueCompareFn, error) {
	switch f {
	case Ascending:
		return calculation.MustCompareValue, nil
	case Descending:
		return calculation.MustReverseCompareValue, nil
	default:
		return nil, fmt.Errorf("unknown sort order %v", f)
	}
}

// CompareCalcValue compares two calculation values.
func (f SortOrder) CompareCalcValue(v1, v2 calculation.ValueUnion) (int, error) {
	switch f {
	case Ascending:
		return calculation.CompareValue(v1, v2)
	case Descending:
		return calculation.CompareValue(v2, v1)
	default:
		return 0, fmt.Errorf("unknown sort order %v", f)
	}
}

// MustCompareCalcValue compares two calculation values, and panics if an error is encountered.
func (f SortOrder) MustCompareCalcValue(v1, v2 calculation.ValueUnion) int {
	res, err := f.CompareCalcValue(v1, v2)
	if err != nil {
		panic(err)
	}
	return res
}

// String returns the string representation of the sort order.
func (f SortOrder) String() string {
	if s, exists := sortOrderBytess[f]; exists {
		return s
	}
	// nolint: goconst
	return "unknown"
}

// UnmarshalJSON unmarshals a JSON object as a sort order.
func (f *SortOrder) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	op, err := newSortOrder(str)
	if err != nil {
		return err
	}
	*f = op
	return nil
}

// ToProto converts a sort order to an optional sort order protobuf message.
func (f *SortOrder) ToProto() (servicepb.OptionalSortOrder, error) {
	if f == nil {
		noValue := &servicepb.OptionalSortOrder_NoValue{NoValue: true}
		return servicepb.OptionalSortOrder{Value: noValue}, nil
	}
	var v servicepb.SortOrder
	switch *f {
	case Ascending:
		v = servicepb.SortOrder_ASCENDING
	case Descending:
		v = servicepb.SortOrder_DESCENDING
	default:
		return servicepb.OptionalSortOrder{}, fmt.Errorf("invalid sort order %v", *f)
	}
	return servicepb.OptionalSortOrder{
		Value: &servicepb.OptionalSortOrder_Data{Data: v},
	}, nil
}

var (
	sortOrderBytess = map[SortOrder]string{
		Ascending:  "ascending",
		Descending: "descending",
	}
	stringToSortOrders map[string]SortOrder
)

func init() {
	stringToSortOrders = make(map[string]SortOrder, len(sortOrderBytess))
	for k, v := range sortOrderBytess {
		stringToSortOrders[v] = k
	}
}
