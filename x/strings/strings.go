package strings

// ArrayFn is the type signature of a string slice clearing fn.
type ArrayFn func(values []string)

// ResetArray clears string pointers inside of a slice of strings.
func ResetArray(values []string) {
	for idx := range values {
		values[idx] = ""
	}
}
