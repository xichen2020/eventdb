package compare

import "bytes"

// BoolCompareFn compares two boolean values.
type BoolCompareFn func(v1, v2 bool) int

// IntCompareFn compares two int values.
type IntCompareFn func(v1, v2 int) int

// DoubleCompareFn compares two double values.
type DoubleCompareFn func(v1, v2 float64) int

// BytesCompareFn compares two string values.
type BytesCompareFn func(v1, v2 []byte) int

// TimeCompareFn compares two time values.
type TimeCompareFn func(v1, v2 int64) int

// BoolCompare compares two boolean values, and returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func BoolCompare(v1, v2 bool) int {
	if v1 == v2 {
		return 0
	}
	if !v1 {
		return -1
	}
	return 1
}

// ReverseBoolCompare reverse compares two boolean values.
func ReverseBoolCompare(v1, v2 bool) int { return BoolCompare(v2, v1) }

// IntCompare compares two int values, and returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func IntCompare(v1, v2 int) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

// ReverseIntCompare reverse compares two boolean values.
func ReverseIntCompare(v1, v2 int) int { return IntCompare(v2, v1) }

// DoubleCompare compares two double values, and returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func DoubleCompare(v1, v2 float64) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

// ReverseDoubleCompare reverse compares two double values.
func ReverseDoubleCompare(v1, v2 float64) int { return DoubleCompare(v2, v1) }

// BytesCompare compares two string values, and returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func BytesCompare(v1, v2 []byte) int {
	return bytes.Compare(v1, v2)
}

// ReverseBytesCompare reverse compares two string values.
func ReverseBytesCompare(v1, v2 []byte) int { return BytesCompare(v2, v1) }

// TimeCompare compares two time values, and returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func TimeCompare(v1, v2 int64) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

// ReverseTimeCompare reverse compares two time values.
func ReverseTimeCompare(v1, v2 int64) int { return TimeCompare(v2, v1) }
