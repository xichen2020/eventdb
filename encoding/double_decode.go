package encoding

// DoubleDecoder decodes double values.
type DoubleDecoder interface {
	// Decode decodes doubles from reader.
	Decode(reader Reader) (ForwardDoubleIterator, error)

	// Reset resets the decoder.
	Reset()
}
