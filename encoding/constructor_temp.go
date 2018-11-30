// Temporary file to hold constructors to ease merge conflicts.

package encoding

import "io"

// STRING

type stringEncoder struct{}

// NewStringEncoder returns a new StringEncoder instance.
func NewStringEncoder() StringEncoder {
	return &stringEncoder{}
}

func (s *stringEncoder) Encode(values StringIterator, writer io.Writer) error {
	return nil
}

func (s *stringEncoder) Reset() {}

type stringDecoder struct{}

// NewStringDecoder returns a new StringDecoder instance.
func NewStringDecoder() StringDecoder {
	return &stringDecoder{}
}

func (s *stringDecoder) Decode(reader Reader) (StringIterator, error) {
	return nil, nil
}

func (s *stringDecoder) Reset() {}

// INT

type intEncoder struct{}

// NewIntEncoder returns a new IntEncoder instance.
func NewIntEncoder() IntEncoder {
	return &intEncoder{}
}

func (s *intEncoder) Encode(values IntIterator, writer io.Writer) error {
	return nil
}

func (s *intEncoder) Reset() {}

type intDecoder struct{}

// NewIntDecoder returns a new IntDecoder instance.
func NewIntDecoder() IntDecoder {
	return &intDecoder{}
}

func (s *intDecoder) Decode(reader Reader) (IntIterator, error) {
	return nil, nil
}

func (s *intDecoder) Reset() {}

// DOUBLE

type doubleEncoder struct{}

// NewDoubleEncoder returns a new DoubleEncoder instance.
func NewDoubleEncoder() DoubleEncoder {
	return &doubleEncoder{}
}

func (s *doubleEncoder) Encode(values DoubleIterator, writer io.Writer) error {
	return nil
}

func (s *doubleEncoder) Reset() {}

type doubleDecoder struct{}

// NewDoubleDecoder returns a new DoubleDecoder instance.
func NewDoubleDecoder() DoubleDecoder {
	return &doubleDecoder{}
}

func (s *doubleDecoder) Decode(reader Reader) (DoubleIterator, error) {
	return nil, nil
}

func (s *doubleDecoder) Reset() {}

// BOOL

type boolEncoder struct{}

// NewBoolEncoder returns a new BoolEncoder instance.
func NewBoolEncoder() BoolEncoder {
	return &boolEncoder{}
}

func (s *boolEncoder) Encode(values BoolIterator, writer io.Writer) error {
	return nil
}

func (s *boolEncoder) Reset() {}

type boolDecoder struct{}

// NewBoolDecoder returns a new BoolDecoder instance.
func NewBoolDecoder() BoolDecoder {
	return &boolDecoder{}
}

func (s *boolDecoder) Decode(reader Reader) (BoolIterator, error) {
	return nil, nil
}

func (s *boolDecoder) Reset() {}
