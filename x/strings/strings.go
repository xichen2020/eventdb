package strings

// ArrayFn is the type signature of a string slice clearing fn.
type ArrayFn func(values []string)

// ResetArray clears string pointers inside of a slice of strings.
func ResetArray(values []string) {
	for idx := range values {
		values[idx] = ""
	}
}

// Equal returns true if two string arrays are considered equal.
func Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
