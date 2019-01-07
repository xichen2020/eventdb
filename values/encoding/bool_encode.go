package encoding

import (
	"io"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/x/proto"
)

const (
	trueByte byte = 1
)

// BoolEncoder encodes bool values.
type BoolEncoder interface {
	// Encode encodes a collection of bools and writes the encoded bytes to the writer.
	Encode(boolVals values.BoolValues, writer io.Writer) error
}

// boolEncoder is a bool encoder.
type boolEncoder struct {
	buf []byte
}

// NewBoolEncoder creates a new bool encoder.
func NewBoolEncoder() BoolEncoder {
	return &boolEncoder{}
}

// Encode encodes a collection of bools and writes the encoded bytes to the writer.
// TODO(xichen): Support bit packing.
func (enc *boolEncoder) Encode(boolVals values.BoolValues, writer io.Writer) error {
	valuesMeta := boolVals.Metadata()
	metaProto := encodingpb.BoolMeta{
		NumTrues:  int32(valuesMeta.NumTrues),
		NumFalses: int32(valuesMeta.NumFalses),
	}
	if err := proto.EncodeBoolMeta(&metaProto, &enc.buf, writer); err != nil {
		return err
	}
	valuesIt, err := boolVals.Iter()
	if err != nil {
		return err
	}
	defer valuesIt.Close()

	return runLengthEncodeBool(valuesIt, writeBool, writer, &enc.buf)
}

func writeBool(writer io.Writer, value bool) error {
	// Write a whole byte since we won't win much by writing a single bit since
	// varints are byte packed.
	var b [1]byte
	if value {
		b[0] = trueByte
	}
	_, err := writer.Write(b[:])
	return err
}
