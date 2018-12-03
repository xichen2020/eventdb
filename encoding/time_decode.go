package encoding

// TimeDecoder decodes time values.
type TimeDecoder interface {
	// Decode decodes times from reader.
	Decode(reader Reader) (ForwardTimeIterator, error)

	// Reset resets the decoder.
	Reset()
}
