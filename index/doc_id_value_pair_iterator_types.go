package index

// NB(xichen): Value-type-specific iterator interfacees for (doc ID, value) pairs
// because genny doesn't support generating typed interfaces.

// DocIDBoolPairIterator iterates over (doc ID, bool value) pairs.
type DocIDBoolPairIterator interface {
	DocIDSetIterator

	// Value returns the current bool value.
	Value() bool
}

// DocIDIntPairIterator iterates over (doc ID, int value) pairs.
type DocIDIntPairIterator interface {
	DocIDSetIterator

	// Value returns the current int value.
	Value() int
}

// DocIDDoublePairIterator iterates over (doc ID, double value) pairs.
type DocIDDoublePairIterator interface {
	DocIDSetIterator

	// Value returns the current double value.
	Value() float64
}

// DocIDStringPairIterator iterates over (doc ID, string value) pairs.
type DocIDStringPairIterator interface {
	DocIDSetIterator

	// Value returns the current string value.
	Value() string
}

// DocIDTimePairIterator iterates over (doc ID, time value) pairs.
type DocIDTimePairIterator interface {
	DocIDSetIterator

	// Value returns the current time value in nanoseconds.
	Value() int64
}
