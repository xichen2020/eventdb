package encoding

// StringDecoder decodes string values.
type StringDecoder interface {
	// Decode decodes strings from reader.
	Decode(reader Reader) (ForwardStringIterator, error)

	// Reset resets the decoder.
	Reset()
}
