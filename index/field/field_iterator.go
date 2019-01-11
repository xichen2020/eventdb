package field

import (
	"github.com/xichen2020/eventdb/index"
)

// NB(xichen): Hand-written typed field iterator interfaces for (doc ID, value) pairs
// because genny doesn't support generating typed interfaces.

// BoolFieldIterator iterates over (doc ID, bool value) pairs in a bool field.
type BoolFieldIterator interface {
	index.DocIDSetIterator

	// Value returns the current bool value.
	Value() bool

	// Err returns the error encountered if any.
	Err() error
}

// IntFieldIterator iterates over (doc ID, int value) pairs in an int field.
type IntFieldIterator interface {
	index.DocIDSetIterator

	// Value returns the current int value.
	Value() int

	// Err returns the error encountered if any.
	Err() error
}

// DoubleFieldIterator iterates over (doc ID, double value) pairs in a double field.
type DoubleFieldIterator interface {
	index.DocIDSetIterator

	// Value returns the current double value.
	Value() float64

	// Err returns the error encountered if any.
	Err() error
}

// StringFieldIterator iterates over (doc ID, string value) pairs in a string field.
type StringFieldIterator interface {
	index.DocIDSetIterator

	// Value returns the current string value.
	Value() string

	// Err returns the error encountered if any.
	Err() error
}

// TimeFieldIterator iterates over (doc ID, time value) pairs in a time field.
type TimeFieldIterator interface {
	index.DocIDSetIterator

	// Value returns the current time value in nanoseconds.
	Value() int64

	// Err returns the error encountered if any.
	Err() error
}
