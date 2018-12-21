package encoding

import "time"

func intSubIntFn(v, v2 int) int {
	return v - v2
}

func intAddIntFn(v, v2 int) int {
	return v + v2
}

func int64SubIntFn(v int64, v2 int) int64 {
	return v - int64(v2)
}

func int64AddIntFn(v int64, v2 int) int64 {
	return v + int64(v2)
}

func int64SubInt64Fn(v, v2 int64) int {
	return int(v - v2)
}

func scaleDownFn(v int64, resolution time.Duration) int64 {
	return v / int64(resolution)
}

func scaleUpFn(v int64, resolution time.Duration) int64 {
	return v * int64(resolution)
}

func intAsUint64Fn(v int) uint64 {
	return uint64(v)
}

func int64AsUint64Fn(v int64) uint64 {
	return uint64(v)
}
