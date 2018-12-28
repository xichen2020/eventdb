package filter

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
