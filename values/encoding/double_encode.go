package encoding

import (
	"io"
	"math"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	xio "github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"
)

const (
	uint64SizeBytes = 8
)

// DoubleEncoder encodes double values.
type DoubleEncoder interface {
	// Encode encodes a collection of doubles and writes the encoded bytes to the writer.
	Encode(doubleVals values.DoubleValues, writer io.Writer) error
}

// doubleEncoder is a double encoder.
type doubleEncoder struct {
	buf []byte
}

// NewDoubleEncoder creates a new double encoder.
func NewDoubleEncoder() DoubleEncoder {
	return &doubleEncoder{
		// Ensure that there is enough buffer size to hold 8 bytes.
		buf: make([]byte, uint64SizeBytes),
	}
}

// Encode encodes a collection of doubles and writes the encoded bytes to the writer.
func (enc *doubleEncoder) Encode(doubleVals values.DoubleValues, writer io.Writer) error {
	valuesMeta := doubleVals.Metadata()
	metaProto := encodingpb.DoubleMeta{
		NumValues: int32(valuesMeta.Size),
		MinValue:  valuesMeta.Min,
		MaxValue:  valuesMeta.Max,
	}
	if err := proto.EncodeDoubleMeta(&metaProto, &enc.buf, writer); err != nil {
		return err
	}
	valuesIt, err := doubleVals.Iter()
	if err != nil {
		return err
	}
	defer valuesIt.Close()

	return encodeDoubleValues(valuesIt, writer, enc.buf)
}

// Precondition: len(buf) >= uint64SizeBytes.
func encodeDoubleValues(
	valuesIt iterator.ForwardDoubleIterator,
	writer io.Writer,
	buf []byte,
) error {
	// Encode doubles as 8 bytes on disk.
	for valuesIt.Next() {
		curr := valuesIt.Current()
		xio.WriteInt(math.Float64bits(curr), uint64SizeBytes, buf)
		if _, err := writer.Write(buf[:uint64SizeBytes]); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}
