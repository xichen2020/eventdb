package compare

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

// StringCompare compares two string values, and returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func StringCompare(v1, v2 string) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

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
