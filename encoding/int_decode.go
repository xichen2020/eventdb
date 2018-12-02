package encoding

// IntDecoder decodes int values.
type IntDecoder interface {
	// Decode decodes ints from reader.
	Decode(reader Reader) (ForwardIntIterator, error)

	// Reset resets the decoder.
	Reset()
}
