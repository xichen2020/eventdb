package query

import (
	"encoding/json"
	"fmt"
)

// FilterCombinator combines multiple filters.
type FilterCombinator int

// A list of supported filter combinators.
const (
	UnknownFilterCombinator FilterCombinator = iota
	And
	Or
)

func newFilterCombinator(str string) (FilterCombinator, error) {
	if f, exists := stringToFilterCombinators[str]; exists {
		return f, nil
	}
	return UnknownFilterCombinator, fmt.Errorf("unknown filter combinator string: %s", str)
}

// String returns the string representation of the filter combinator.
func (f FilterCombinator) String() string {
	if s, exists := filterCombinatorStrings[f]; exists {
		return s
	}
	// nolint: goconst
	return "unknown"
}

// UnmarshalJSON unmarshals a JSON object as a filter combinator.
func (f *FilterCombinator) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	op, err := newFilterCombinator(str)
	if err != nil {
		return err
	}
	*f = op
	return nil
}

var (
	filterCombinatorStrings = map[FilterCombinator]string{
		And: "AND",
		Or:  "OR",
	}
	stringToFilterCombinators map[string]FilterCombinator
)

func init() {
	stringToFilterCombinators = make(map[string]FilterCombinator, len(filterCombinatorStrings))
	for k, v := range filterCombinatorStrings {
		stringToFilterCombinators[v] = k
	}
}
