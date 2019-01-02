package filter

import (
	"math"
)

// DoubleFilter matches against double values.
type DoubleFilter interface {
	// Match returns true if the given value is considered a match.
	Match(v float64) bool
}

const (
	// doubleEps is used to determine whether two floating point numbers are considered equal.
	doubleEps = 1e-10
)

type doubleToDoubleFilterFn func(float64) doubleFilterFn
type intToDoubleFilterFn func(int) doubleFilterFn

type doubleFilterFn func(v float64) bool

func (fn doubleFilterFn) Match(v float64) bool { return fn(v) }

func equalsDoubleDouble(rhs float64) doubleFilterFn {
	return func(lhs float64) bool {
		return math.Abs(lhs-rhs) < doubleEps
	}
}

func notEqualsDoubleDouble(rhs float64) doubleFilterFn {
	return func(lhs float64) bool {
		return math.Abs(lhs-rhs) >= doubleEps
	}
}

func largerThanDoubleDouble(rhs float64) doubleFilterFn {
	return func(lhs float64) bool {
		return lhs > rhs
	}
}

func largerThanOrEqualDoubleDouble(rhs float64) doubleFilterFn {
	return func(lhs float64) bool {
		return lhs >= rhs
	}
}

func smallerThanDoubleDouble(rhs float64) doubleFilterFn {
	return func(lhs float64) bool {
		return lhs < rhs
	}
}

func smallerThanOrEqualDoubleDouble(rhs float64) doubleFilterFn {
	return func(lhs float64) bool {
		return lhs <= rhs
	}
}

func equalsDoubleInt(rhs int) doubleFilterFn {
	rf := float64(rhs)
	return func(lhs float64) bool {
		return math.Abs(lhs-rf) < doubleEps
	}
}

func notEqualsDoubleInt(rhs int) doubleFilterFn {
	rf := float64(rhs)
	return func(lhs float64) bool {
		return math.Abs(lhs-rf) >= doubleEps
	}
}

func largerThanDoubleInt(rhs int) doubleFilterFn {
	rf := float64(rhs)
	return func(lhs float64) bool {
		return lhs > rf
	}
}

func largerThanOrEqualDoubleInt(rhs int) doubleFilterFn {
	rf := float64(rhs)
	return func(lhs float64) bool {
		return lhs >= rf
	}
}

func smallerThanDoubleInt(rhs int) doubleFilterFn {
	rf := float64(rhs)
	return func(lhs float64) bool {
		return lhs < rf
	}
}

func smallerThanOrEqualDoubleInt(rhs int) doubleFilterFn {
	rf := float64(rhs)
	return func(lhs float64) bool {
		return lhs <= rf
	}
}
