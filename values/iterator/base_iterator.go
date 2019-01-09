package iterator

import "io"

// nolint: megacheck
type baseIterator interface {
	io.Closer

	// Next returns true if there is another value in the data stream.
	// If it returns false, err should be checked for errors.
	Next() bool

	// Err returns any error encountered during iteration.
	Err() error
}
