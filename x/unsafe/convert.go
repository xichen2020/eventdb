package unsafe

import (
	"reflect"
	"unsafe"
)

// ToString converts a byte slice to a string with zero allocation.
// NB: The byte slice is fully owned by the string returned and must not be mutated.
// Adapted from https://golang.org/src/strings/builder.go#46.
func ToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// ToBytes converts a string to a byte slice with zero allocation.
// NB: The byte slice should not be mutated in any way.
// NB: This should work in theory but have not been stress tested.
func ToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}

	// NB(xichen): we need to declare a real byte slice so internally the compiler
	// knows to use an unsafe.Pointer to keep track of the underlying memory so tha
	// once the slice's array pointer is updated with the pointer to the string's
	// underlying bytes, the compiler won't prematurely GC the memory when the string
	// goes out of scope.
	var b []byte
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	// NB(xichen): this makes sure that even if GC relocates the string's underlying
	// memory after this assignment, the corresponding unsafe.Pointer in the internal
	// slice struct will be updated accordingly to reflect the memory relocation.
	byteHeader.Data = (*reflect.StringHeader)(unsafe.Pointer(&s)).Data

	// NB(xichen): it is important that we access s after we assign the Data
	// pointer of the string header to the Data pointer of the slice header to
	// make sure the string (and the underlying bytes backing the string) don't get
	// GC'ed before the assignment happens.
	l := len(s)
	byteHeader.Len = l
	byteHeader.Cap = l

	return b
}
