package convert

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
