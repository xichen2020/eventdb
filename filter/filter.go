package filter

// TimeFilter matches against time values.
type TimeFilter interface {
	// Match returns true if the given value is considered a match.
	Match(v int64) bool
}
