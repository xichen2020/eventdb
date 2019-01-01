package filter

import (
	"math"

	"github.com/xichen2020/eventdb/x/convert"
)

// IntFilter matches against int values.
type IntFilter interface {
	// Match returns true if the given value is considered a match.
	Match(v int) bool
}

type intToIntFilterFn func(int) intFilterFn
type doubleToIntFilterFn func(float64) intFilterFn

type intFilterFn func(v int) bool

func (fn intFilterFn) Match(v int) bool { return fn(v) }

func equalsIntInt(rhs int) intFilterFn {
	return func(lhs int) bool {
		return lhs == rhs
	}
}

func notEqualsIntInt(rhs int) intFilterFn {
	return func(lhs int) bool {
		return lhs != rhs
	}
}

func largerThanIntInt(rhs int) intFilterFn {
	return func(lhs int) bool {
		return lhs > rhs
	}
}

func largerThanOrEqualIntInt(rhs int) intFilterFn {
	return func(lhs int) bool {
		return lhs >= rhs
	}
}

func smallerThanIntInt(rhs int) intFilterFn {
	return func(lhs int) bool {
		return lhs < rhs
	}
}

func smallerThanOrEqualIntInt(rhs int) intFilterFn {
	return func(lhs int) bool {
		return lhs <= rhs
	}
}

func equalsIntDouble(rhs float64) intFilterFn {
	ri, ok := convert.TryAsInt(rhs)
	if !ok {
		return func(int) bool { return false }
	}
	return func(lhs int) bool {
		return lhs == ri
	}
}

func notEqualsIntDouble(rhs float64) intFilterFn {
	ri, ok := convert.TryAsInt(rhs)
	if !ok {
		return func(int) bool { return true }
	}
	return func(lhs int) bool {
		return lhs != ri
	}
}

func largerThanIntDouble(rhs float64) intFilterFn {
	ri := int(math.Floor(rhs))
	return func(lhs int) bool {
		return lhs > ri
	}
}

func largerThanOrEqualIntDouble(rhs float64) intFilterFn {
	ri := int(math.Ceil(rhs))
	return func(lhs int) bool {
		return lhs >= ri
	}
}

func smallerThanIntDouble(rhs float64) intFilterFn {
	ri := int(math.Ceil(rhs))
	return func(lhs int) bool {
		return lhs < ri
	}
}

func smallerThanOrEqualIntDouble(rhs float64) intFilterFn {
	ri := int(math.Floor(rhs))
	return func(lhs int) bool {
		return lhs <= ri
	}
}
