package filter

import (
	"strings"
)

// StringFilter matches against string values.
type StringFilter interface {
	// Match returns true if the given value is considered a match.
	Match(v string) bool
}

type stringToStringFilterFn func(string) stringFilterFn

type stringFilterFn func(v string) bool

func (fn stringFilterFn) Match(v string) bool { return fn(v) }

func equalsStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return lhs == rhs
	}
}

func notEqualsStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return lhs != rhs
	}
}

func largerThanStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return lhs > rhs
	}
}

func largerThanOrEqualStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return lhs >= rhs
	}
}

func smallerThanStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return lhs < rhs
	}
}

func smallerThanOrEqualStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return lhs <= rhs
	}
}

func startsWithStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return strings.HasPrefix(lhs, rhs)
	}
}

func doesNotStartWithStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return !strings.HasPrefix(lhs, rhs)
	}
}

func endsWithStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return strings.HasSuffix(lhs, rhs)
	}
}

func doesNotEndWithStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return !strings.HasSuffix(lhs, rhs)
	}
}

func containsStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return strings.Contains(lhs, rhs)
	}
}

func doesNotContainStringString(rhs string) stringFilterFn {
	return func(lhs string) bool {
		return !strings.Contains(lhs, rhs)
	}
}
