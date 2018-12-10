package query

import (
	"encoding/json"
	"fmt"
)

// CalculationOp represents a calculation operator.
type CalculationOp int

// A list of supported calculation operators.
const (
	UnknownCalculationOp CalculationOp = iota
	Count
	Sum
	Avg
	CountDistinct
	Max
	Min
	P99
)

func newCalculationOp(str string) (CalculationOp, error) {
	if f, exists := stringToCalculationOps[str]; exists {
		return f, nil
	}
	return UnknownCalculationOp, fmt.Errorf("unknown calculation op string: %s", str)
}

// String returns the string representation of the calculation operator.
func (f CalculationOp) String() string {
	if s, exists := calculationOpStrings[f]; exists {
		return s
	}
	// nolint: goconst
	return "unknown"
}

// UnmarshalJSON unmarshals a JSON object as a calculation operator.
func (f *CalculationOp) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	op, err := newCalculationOp(str)
	if err != nil {
		return err
	}
	*f = op
	return nil
}

var (
	calculationOpStrings = map[CalculationOp]string{
		Count:         "COUNT",
		Sum:           "SUM",
		Avg:           "AVG",
		CountDistinct: "COUNT_DISTINCT",
		Max:           "MAX",
		Min:           "MIN",
		P99:           "P99",
	}
	stringToCalculationOps map[string]CalculationOp
)

func init() {
	stringToCalculationOps = make(map[string]CalculationOp, len(calculationOpStrings))
	for k, v := range calculationOpStrings {
		stringToCalculationOps[v] = k
	}
}
