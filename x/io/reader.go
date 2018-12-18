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

// SkippableReader embeds `Reader` iface and implements a `Skip` method to skip
// underlying blocks of data.
type SkippableReader interface {
	Skip() error

	Reader
}
