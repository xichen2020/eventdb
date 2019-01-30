package filter

import (
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
)

// IntIsInRange returns true if filterVal is within the range defined in IntMeta.
func (f Op) IntIsInRange(meta encodingpb.IntMeta, filterVal int) bool {
	var (
		max = int(meta.MaxValue)
		min = int(meta.MinValue)
	)
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

// StringIsInRange returns true if filterVal is within the range defined in StringMeta.
func (f Op) StringIsInRange(meta encodingpb.StringMeta, filterVal string) bool {
	var (
		max = meta.MaxValue
		min = meta.MinValue
	)
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

// BoolIsInRange returns true if filterVal is within the range defined in BoolMeta.
func (f Op) BoolIsInRange(meta encodingpb.BoolMeta, filterVal bool) bool {
	var (
		numTrue  = meta.NumTrues
		numFalse = meta.NumFalses
	)
	switch f {
	case Equals:
		if filterVal && numTrue == 0 {
			return false
		}
		if !filterVal && numFalse == 0 {
			return false
		}
	case NotEquals:
		if filterVal && numFalse == 0 {
			return false
		}
		if !filterVal && numTrue == 0 {
			return false
		}
	}
	return true
}

// DoubleIsInRange returns true if filterVal is within the range defined in DoubleMeta.
func (f Op) DoubleIsInRange(meta encodingpb.DoubleMeta, filterVal float64) bool {
	var (
		max = meta.MaxValue
		min = meta.MinValue
	)
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

// TimeIsInRange returns true if filterVal is within the range defined in TimeMeta.
func (f Op) TimeIsInRange(meta encodingpb.TimeMeta, filterVal int64) bool {
	var (
		max = meta.MaxValue
		min = meta.MinValue
	)
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
