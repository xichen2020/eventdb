package io

import (
	"io"

	"github.com/m3db/m3/src/x/close"
)

// Reader embeds both `io.Reader` and `io.ByteReader`.
type Reader interface {
	io.Reader
	io.ByteReader
}

// SimpleReadCloser embeds both `Reader` and `close.SimpleCloser`.
type SimpleReadCloser interface {
	Reader
	close.SimpleCloser
}

// ReaderNoopCloser is a reader that has a no-op Close method.
type ReaderNoopCloser struct {
	Reader
}

// NewReaderNoopCloser returns a new reader that implements a no-op Close.
func NewReaderNoopCloser(r Reader) SimpleReadCloser {
	return ReaderNoopCloser{Reader: r}
}

// Close is a no-op.
func (r ReaderNoopCloser) Close() {}
