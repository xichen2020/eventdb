package convert

import "time"

// TryAsInt try to convert a floating point number to
// an integer, returning the integral value and true
// if the given number is indeed an integer, and false otherwise.
func TryAsInt(v float64) (int, bool) {
	i := int(v)
	if v == float64(i) {
		return i, true
	}
	return 0, false
}

// IntAddIntFn adds two ints together.
func IntAddIntFn(v, v2 int) int {
	return v + v2
}

// IntSubIntFn subtracts v2 from v.
func IntSubIntFn(v, v2 int) int {
	return v - v2
}

// Int64AddIntFn adds an int64 to a int.
func Int64AddIntFn(v int64, v2 int) int64 {
	return v + int64(v2)
}

// Int64SubInt64Fn subtracts an int64 from an int64.
func Int64SubInt64Fn(v, v2 int64) int {
	return int(v - v2)
}

// ScaleDownTimeFn scales down the time value by the resolution.
func ScaleDownTimeFn(v int64, resolution time.Duration) int64 {
	return v / int64(resolution)
}

// ScaleUpTimeFn scales up the time value by the resolution.
func ScaleUpTimeFn(v int64, resolution time.Duration) int64 {
	return v * int64(resolution)
}

// IntAsUint64Fn converts an int as a uint64.
func IntAsUint64Fn(v int) uint64 {
	return uint64(v)
}

// Int64AsUint64Fn converts an int64 as a uint64.
func Int64AsUint64Fn(v int64) uint64 {
	return uint64(v)
}
