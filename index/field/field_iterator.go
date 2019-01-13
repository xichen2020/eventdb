package field

import (
	"github.com/xichen2020/eventdb/document/field"
)

// BaseFieldIterator is the base field iterator.
type BaseFieldIterator interface {
	// Next returns true if there are more field values to be iterated over.
	Next() bool

	// DocID returns the current doc ID.
	DocID() int32

	// ValueUnion returns the current value as a value union.
	ValueUnion() field.ValueUnion

	// Err returns the error encountered if any.
	Err() error

	// Close closes the iterator.
	Close()
}

// NullFieldIterator iterates over (doc ID, null value) pairs in a null field.
type NullFieldIterator interface {
	BaseFieldIterator
}

// BoolFieldIterator iterates over (doc ID, bool value) pairs in a bool field.
type BoolFieldIterator interface {
	BaseFieldIterator

	// Value returns the current bool value.
	Value() bool
}

// IntFieldIterator iterates over (doc ID, int value) pairs in an int field.
type IntFieldIterator interface {
	BaseFieldIterator

	// Value returns the current bool value.
	Value() int
}

// DoubleFieldIterator iterates over (doc ID, double value) pairs in a double field.
type DoubleFieldIterator interface {
	BaseFieldIterator

	// Value returns the current bool value.
	Value() float64
}

// StringFieldIterator iterates over (doc ID, string value) pairs in a string field.
type StringFieldIterator interface {
	BaseFieldIterator

	// Value returns the current bool value.
	Value() string
}

// TimeFieldIterator iterates over (doc ID, time value) pairs in a time field.
type TimeFieldIterator interface {
	BaseFieldIterator

	// Value returns the current bool value.
	Value() int64
}
