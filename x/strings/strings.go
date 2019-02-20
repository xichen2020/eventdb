package strings

// ClearArrayFn is the type signature of a string slice clearing fn.
type ClearArrayFn func(values []string)

// ValuesResetFn clears string pointers inside of a slice of strings.
func ValuesResetFn(values []string) {
	for idx := range values {
		values[idx] = ""
	}
}
