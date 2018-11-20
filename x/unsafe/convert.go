package unsafe

import (
	"unsafe"
)

// ToString converts a byte slice to a string with zero allocation.
// NB: The byte slice is fully owned by the string returned and must not be mutated.
// Adapted from https://golang.org/src/strings/builder.go#46.
func ToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
