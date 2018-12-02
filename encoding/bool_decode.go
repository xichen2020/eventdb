package encoding

// BoolDecoder decodes bool values.
type BoolDecoder interface {
	// Decode decodes bools from reader.
	Decode(reader Reader) (ForwardBoolIterator, error)

	// Reset resets the decoder.
	Reset()
}
