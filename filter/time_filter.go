package filter

// TimeFilter matches against time values.
type TimeFilter interface {
	// Match returns true if the given value is considered a match.
	Match(v int64) bool
}

type timeToTimeFilterFn func(int64) timeFilterFn

type timeFilterFn func(v int64) bool

func (fn timeFilterFn) Match(v int64) bool { return fn(v) }

func equalsTimeTime(rhs int64) timeFilterFn {
	return func(lhs int64) bool {
		return lhs == rhs
	}
}

func notEqualsTimeTime(rhs int64) timeFilterFn {
	return func(lhs int64) bool {
		return lhs != rhs
	}
}

func largerThanTimeTime(rhs int64) timeFilterFn {
	return func(lhs int64) bool {
		return lhs > rhs
	}
}

func largerThanOrEqualTimeTime(rhs int64) timeFilterFn {
	return func(lhs int64) bool {
		return lhs >= rhs
	}
}

func smallerThanTimeTime(rhs int64) timeFilterFn {
	return func(lhs int64) bool {
		return lhs < rhs
	}
}

func smallerThanOrEqualTimeTime(rhs int64) timeFilterFn {
	return func(lhs int64) bool {
		return lhs <= rhs
	}
}

// TimeRangeFilter is a time range filter that matches time values
// in [startNanosInclusive, endNanosExclusive).
type TimeRangeFilter struct {
	startNanosInclusive int64
	endNanosExclusive   int64
}

// NewTimeRangeFilter creates a new time range filter.
func NewTimeRangeFilter(
	startNanosInclusive int64,
	endNanosExclusive int64,
) *TimeRangeFilter {
	return &TimeRangeFilter{
		startNanosInclusive: startNanosInclusive,
		endNanosExclusive:   endNanosExclusive,
	}
}

// Match returns true if the time value in nanoseconds is in [start, end).
func (f *TimeRangeFilter) Match(timeNanos int64) bool {
	return timeNanos >= f.startNanosInclusive && timeNanos < f.endNanosExclusive
}
