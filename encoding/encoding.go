package encoding

import (
	"io"
)

// StringEncoder encodes string values.
type StringEncoder interface {
	// Encode a collection of strings.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Encode`.
	Encode(values StringIterator, writer io.Writer) error
	// Reset should be called between `Encode` calls.
	Reset()
}

// StringDecoder decodes int values.
type StringDecoder interface {
	// Decode the source bytes.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Decode`.
	Decode(src Reader) (StringIterator, error)
	// Reset should be called between `Decode` calls.
	Reset()
}

// StringIterator lazily produces int values decoded from a byte stream.
type StringIterator interface {
	BaseIterator

	// Current returns the current value in the iteration.
	Current() string
	// Reset internal iterator data.
	Reset(values []string)
}

// IntEncoder encodes int values.
type IntEncoder interface {
	// Encode a collection of ints.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Encode`.
	Encode(values IntIterator, writer io.Writer) error
	// Reset should be called between `Encode` calls.
	Reset()
}

// IntDecoder decodes int values.
type IntDecoder interface {
	// Decode the source bytes.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Decode`.
	Decode(src Reader) (IntIterator, error)
	// Reset should be called between `Decode` calls.
	Reset()
}

// IntIterator lazily produces int values decoded from a byte stream.
type IntIterator interface {
	BaseIterator

	// Current returns the current value in the iteration.
	Current() int
	// Reset internal iterator data.
	Reset(values []int)
}

// DoubleEncoder encodes float64 values.
type DoubleEncoder interface {
	// Encode a collection of float64s.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Encode`.
	Encode(values DoubleIterator, writer io.Writer) error
	// Reset should be called between `Encode` calls.
	Reset()
}

// DoubleDecoder decodes float64 values.
type DoubleDecoder interface {
	// Decode the source bytes.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Decode`.
	Decode(src Reader) (DoubleIterator, error)
	// Reset should be called between `Decode` calls.
	Reset()
}

// DoubleIterator lazily produces float64 values decoded from a byte stream.
type DoubleIterator interface {
	BaseIterator

	// Current returns the current value in the iteration.
	Current() float64
	// Reset internal iterator data.
	Reset(values []float64)
}

// BoolEncoder encodes bool values.
type BoolEncoder interface {
	// Encode a collection of bools.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Encode`.
	Encode(values BoolIterator, writer io.Writer) error
	// Reset should be called between `Encode` calls.
	Reset()
}

// BoolDecoder decodes bool values.
type BoolDecoder interface {
	// Decode the source bytes.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Decode`.
	Decode(src Reader) (BoolIterator, error)
	// Reset should be called between `Decode` calls.
	Reset()
}

// BoolIterator lazily produces bool values decoded from a byte stream.
type BoolIterator interface {
	BaseIterator

	// Current returns the current value in the iteration.
	Current() bool
	// Reset internal iterator data.
	Reset(values []bool)
}

// BaseIterator forms the base interface for all type specific iterators.
type BaseIterator interface {
	io.Closer

	// Next returns true if there is another value
	// in the data stream. If it returns false, err should be
	// checked for errors.
	Next() bool
	// Err returns any error encountered during iteration.
	Err() error
	// Rewind iteration back to the first item in the collection.
	Rewind()
}

// Reader wrap both io.Reader and io.ByteReader ifaces.
type Reader interface {
	io.Reader
	io.ByteReader
}
