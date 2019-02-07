package io

import "io"

// Reader embeds both `io.Reader` and `io.ByteReader`.
type Reader interface {
	io.Reader
	io.ByteReader
}
