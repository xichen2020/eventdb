package bytes

// ArrayFn is the type signature of a string slice clearing fn.
type ArrayFn func(values [][]byte)

// ResetArray clears string pointers inside of a slice of strings.
func ResetArray(values [][]byte) {
	for idx := range values {
		values[idx] = nil
	}
}
