package filter

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/generated/proto/servicepb"
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
	if s, exists := filterCombinatorBytes[f]; exists {
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

// ToProto converts a filter combinator to an optional filter combinator protobuf message.
func (f *Combinator) ToProto() (servicepb.OptionalFilterCombinator, error) {
	if f == nil {
		noValue := &servicepb.OptionalFilterCombinator_NoValue{NoValue: true}
		return servicepb.OptionalFilterCombinator{Value: noValue}, nil
	}
	var v servicepb.FilterCombinator
	switch *f {
	case And:
		v = servicepb.FilterCombinator_AND
	case Or:
		v = servicepb.FilterCombinator_OR
	default:
		return servicepb.OptionalFilterCombinator{}, fmt.Errorf("invalid filter combinator %v", *f)
	}
	return servicepb.OptionalFilterCombinator{
		Value: &servicepb.OptionalFilterCombinator_Data{Data: v},
	}, nil
}

var (
	filterCombinatorBytes = map[Combinator]string{
		And: "AND",
		Or:  "OR",
	}
	stringToCombinators map[string]Combinator
)

func init() {
	stringToCombinators = make(map[string]Combinator, len(filterCombinatorBytes))
	for k, v := range filterCombinatorBytes {
		stringToCombinators[v] = k
	}
}
