package strings

// ValuesResetFn clears string pointers inside of a slice of strings.
func ValuesResetFn(values []string) {
	for idx := range values {
		values[idx] = ""
	}
}
