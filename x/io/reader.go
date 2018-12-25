package io

import "io"

// Reader embeds both `io.Reader` and `io.ByteReader` ifaces.
type Reader interface {
	io.Reader
	io.ByteReader
}

// SeekableReader embeds both `io.Seeker` and `Reader` ifaces.
type SeekableReader interface {
	Reader
	io.Seeker
}
