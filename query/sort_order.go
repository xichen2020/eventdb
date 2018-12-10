package query

import (
	"encoding/json"
	"fmt"
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
