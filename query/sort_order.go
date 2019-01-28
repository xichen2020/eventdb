package query

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/calculation"

	"github.com/xichen2020/eventdb/document/field"
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

// CompareFieldValue compares two values, and
// * returns -1 if `v1` should come before `v2` in the order specified;
// * returns 1 if `v1` should come after `v2` in the order specified;
// * returns 0 if `v1` and `v2` have the same rank in the order specified.
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

// CompareCalcValue compares two calculation values, and
// * returns -1 if `v1` should come before `v2` in the order specified;
// * returns 1 if `v1` should come after `v2` in the order specified;
// * returns 0 if `v1` and `v2` have the same rank in the order specified.
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
	if s, exists := sortOrderStrings[f]; exists {
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

var (
	sortOrderStrings = map[SortOrder]string{
		Ascending:  "ascending",
		Descending: "descending",
	}
	stringToSortOrders map[string]SortOrder
)

func init() {
	stringToSortOrders = make(map[string]SortOrder, len(sortOrderStrings))
	for k, v := range sortOrderStrings {
		stringToSortOrders[v] = k
	}
}
