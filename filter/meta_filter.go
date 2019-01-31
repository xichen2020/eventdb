package filter

// IntMaybeInRange returns true if filterVal is within the range defined by min and max.
// If this returns false, then we can bail early; but if it returns true, it doesn't necessarily
// mean the filterVal exists in the values that this filter is acting on.
func (f Op) IntMaybeInRange(min, max, filterVal int) bool {
	switch f {
	case Equals:
		if filterVal > max || filterVal < min {
			return false
		}
	case LargerThan:
		if filterVal >= max {
			return false
		}
	case LargerThanOrEqual:
		if filterVal > max {
			return false
		}
	case SmallerThan:
		if filterVal <= min {
			return false
		}
	case SmallerThanOrEqual:
		if filterVal < min {
			return false
		}
	}
	return true
}

// StringMaybeInRange returns true if filterVal is within the range defined in by min and max.
// If this returns false, then we can bail early; but if it returns true, it doesn't necessarily
// mean the filterVal exists in the values that this filter is acting on.
func (f Op) StringMaybeInRange(min, max, filterVal string) bool {
	switch f {
	case Equals, StartsWith:
		if filterVal > max || filterVal < min {
			return false
		}
	case LargerThan:
		if filterVal >= max {
			return false
		}
	case LargerThanOrEqual:
		if filterVal > max {
			return false
		}
	case SmallerThan:
		if filterVal <= min {
			return false
		}
	case SmallerThanOrEqual:
		if filterVal < min {
			return false
		}
	}
	return true
}

// BoolIsInRange returns true if filterVal exists in the values that this filter is acting on.
func (f Op) BoolIsInRange(numTrues, numFalses int, filterVal bool) bool {
	switch f {
	case Equals:
		if filterVal && numTrues == 0 {
			return false
		}
		if !filterVal && numFalses == 0 {
			return false
		}
	case NotEquals:
		if filterVal && numFalses == 0 {
			return false
		}
		if !filterVal && numTrues == 0 {
			return false
		}
	}
	return true
}

// DoubleMaybeInRange returns true if filterVal is within the range defined by min and max.
// If this returns false, then we can bail early; but if it returns true, it doesn't necessarily
// mean the filterVal exists in the values that this filter is acting on.
func (f Op) DoubleMaybeInRange(min, max, filterVal float64) bool {
	switch f {
	case Equals:
		if filterVal > max || filterVal < min {
			return false
		}
	case LargerThan:
		if filterVal >= max {
			return false
		}
	case LargerThanOrEqual:
		if filterVal > max {
			return false
		}
	case SmallerThan:
		if filterVal <= min {
			return false
		}
	case SmallerThanOrEqual:
		if filterVal < min {
			return false
		}
	}
	return true
}

// TimeMaybeInRange returns true if filterVal is within the range defined by min and max.
// If this returns false, then we can bail early; but if it returns true, it doesn't necessarily
// mean the filterVal exists in the values that this filter is acting on.
func (f Op) TimeMaybeInRange(min, max, filterVal int64) bool {
	switch f {
	case Equals:
		if filterVal > max || filterVal < min {
			return false
		}
	case LargerThan:
		if filterVal >= max {
			return false
		}
	case LargerThanOrEqual:
		if filterVal > max {
			return false
		}
	case SmallerThan:
		if filterVal <= min {
			return false
		}
	case SmallerThanOrEqual:
		if filterVal < min {
			return false
		}
	}
	return true
}
