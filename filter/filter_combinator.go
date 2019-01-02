package filter

import (
	"encoding/json"
	"fmt"
)

// Combinator combines multiple filters.
type Combinator int

// A list of supported filter combinators.
const (
	UnknownCombinator Combinator = iota
	And
	Or
)

func newCombinator(str string) (Combinator, error) {
	if f, exists := stringToCombinators[str]; exists {
		return f, nil
	}
	return UnknownCombinator, fmt.Errorf("unknown filter combinator string: %s", str)
}

// String returns the string representation of the filter combinator.
func (f Combinator) String() string {
	if s, exists := filterCombinatorStrings[f]; exists {
		return s
	}
	// nolint: goconst
	return "unknown"
}

// UnmarshalJSON unmarshals a JSON object as a filter combinator.
func (f *Combinator) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	op, err := newCombinator(str)
	if err != nil {
		return err
	}
	*f = op
	return nil
}

var (
	filterCombinatorStrings = map[Combinator]string{
		And: "AND",
		Or:  "OR",
	}
	stringToCombinators map[string]Combinator
)

func init() {
	stringToCombinators = make(map[string]Combinator, len(filterCombinatorStrings))
	for k, v := range filterCombinatorStrings {
		stringToCombinators[v] = k
	}
}
