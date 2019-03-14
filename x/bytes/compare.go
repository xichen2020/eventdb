package bytes

import (
	"bytes"
)

const (
	compareResultGreaterThan = 1
	compareResultLessThan    = -1
)

// GreaterThan returns true if v1 > v2.
func GreaterThan(v1, v2 []byte) bool {
	return bytes.Compare(v1, v2) == compareResultGreaterThan
}

// GreaterThanOrEqual returns true if v1 > v2 || v1 == v2.
func GreaterThanOrEqual(v1, v2 []byte) bool {
	return bytes.Compare(v1, v2) != compareResultLessThan
}

// LessThan returns true if v1 < v2.
func LessThan(v1, v2 []byte) bool {
	return bytes.Compare(v1, v2) == compareResultLessThan
}

// LessThanOrEqual returns true if v1 < v2 || v1 == v2.
func LessThanOrEqual(v1, v2 []byte) bool {
	return bytes.Compare(v1, v2) != compareResultGreaterThan
}
