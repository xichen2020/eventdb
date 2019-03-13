package field

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/bytes"
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

// MaskingFieldIterator is a field iterator that is produced from a masking iterator
// applied on top of field data.
type MaskingFieldIterator interface {
	// MaskingPosition returns the current position in the masking iterator.
	MaskingPosition() int
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

	// Value returns the current int value.
	Value() int
}

// DoubleFieldIterator iterates over (doc ID, double value) pairs in a double field.
type DoubleFieldIterator interface {
	BaseFieldIterator

	// Value returns the current double value.
	Value() float64
}

// BytesFieldIterator iterates over (doc ID, string value) pairs in a string field.
type BytesFieldIterator interface {
	BaseFieldIterator

	// Value returns the current string value.
	Value() bytes.Bytes
}

// TimeFieldIterator iterates over (doc ID, time value) pairs in a time field.
type TimeFieldIterator interface {
	BaseFieldIterator

	// Value returns the current time value.
	Value() int64
}

// MaskingNullFieldIterator a field iterator that is produced from a masking iterator.
type MaskingNullFieldIterator interface {
	NullFieldIterator
	MaskingFieldIterator
}

// MaskingBoolFieldIterator iterates over (doc ID, bool value) pairs in a bool field.
type MaskingBoolFieldIterator interface {
	BoolFieldIterator
	MaskingFieldIterator
}

// MaskingIntFieldIterator iterates over (doc ID, int value) pairs in an int field.
type MaskingIntFieldIterator interface {
	IntFieldIterator
	MaskingFieldIterator
}

// MaskingDoubleFieldIterator iterates over (doc ID, double value) pairs in a double field.
type MaskingDoubleFieldIterator interface {
	DoubleFieldIterator
	MaskingFieldIterator
}

// MaskingBytesFieldIterator iterates over (doc ID, string value) pairs in a string field.
type MaskingBytesFieldIterator interface {
	BytesFieldIterator
	MaskingFieldIterator
}

// MaskingTimeFieldIterator iterates over (doc ID, time value) pairs in a time field.
type MaskingTimeFieldIterator interface {
	TimeFieldIterator
	MaskingFieldIterator
}
