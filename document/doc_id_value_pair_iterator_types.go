package document

// NB(xichen): Value-type-specific iterator interfacees for (doc ID, value) pairs
// because genny doesn't support generating typed interfaces.

// DocIDTimePairIterator iterates over (doc ID, time value) pairs.
type DocIDTimePairIterator interface {
	DocIDSetIterator

	// Value returns the current time value in nanoseconds.
	Value() int64
}
