package query

import (
	"encoding/json"
	"fmt"
)

// FilterOp represents a filter operator.
type FilterOp int

// A list of supported filter operators.
const (
	UnknownFilterOp FilterOp = iota
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
	Exists
	DoesNotExist
	Contains
	DoesNotContain
	IsNull
	IsNotNull
)

func newFilterOp(str string) (FilterOp, error) {
	if f, exists := stringToFilterOps[str]; exists {
		return f, nil
	}
	return UnknownFilterOp, fmt.Errorf("unknown filter op string: %s", str)
}

// String returns the string representation of the filter operator.
func (f FilterOp) String() string {
	if s, exists := filterOpStrings[f]; exists {
		return s
	}
	// nolint: goconst
	return "unknown"
}

// UnmarshalJSON unmarshals a JSON object as a filter operator.
func (f *FilterOp) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	op, err := newFilterOp(str)
	if err != nil {
		return err
	}
	*f = op
	return nil
}

var (
	filterOpStrings = map[FilterOp]string{
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
		Exists:             "exists",
		DoesNotExist:       "notExists",
		Contains:           "contains",
		DoesNotContain:     "notContains",
		IsNull:             "isNull",
		IsNotNull:          "isNotNull",
	}
	stringToFilterOps map[string]FilterOp
)

func init() {
	stringToFilterOps = make(map[string]FilterOp, len(filterOpStrings))
	for k, v := range filterOpStrings {
		stringToFilterOps[v] = k
	}
}
