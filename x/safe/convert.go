package safe

// ToString safely converts a byte slice to a string.
// TODO: This method should be removed when we move over to handling bytes in the
// database hot path instead of strings.
func ToString(b []byte) string {
	return string(b)
}
