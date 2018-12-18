package encoding

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
