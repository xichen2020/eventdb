package filter

// BoolFilter matches against bool values.
type BoolFilter interface {
	// Match returns true if the given value is considered a match.
	Match(v bool) bool
}

type boolToBoolFilterFn func(bool) boolFilterFn

type boolFilterFn func(v bool) bool

func (fn boolFilterFn) Match(v bool) bool { return fn(v) }

func equalsBoolBool(rhs bool) boolFilterFn {
	return func(lhs bool) bool {
		return lhs == rhs
	}
}

func notEqualsBoolBool(rhs bool) boolFilterFn {
	return func(lhs bool) bool {
		return lhs != rhs
	}
}
