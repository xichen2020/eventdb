package encoding

import (
	"encoding/binary"
	"io"
	"math"
	"math/bits"

	"github.com/gogo/protobuf/proto"
)

// Constants.
const (
	Uint32SizeBytes = 4
	Uint64SizeBytes = 8
)

// Endianness.
var (
	endianness = binary.LittleEndian
)

// ProtoMessage is a proto.Message w/ extra gogoprotobuf generated methods.
type ProtoMessage interface {
	proto.Message

	Unmarshal(dst []byte) error
	MarshalTo(dst []byte) (int, error)
	Size() int
}

// Reader is both an io.Reader and an io.ByteReader.
type Reader interface {
	io.Reader
	io.ByteReader
}

// ProtoEncode a proto message to an io.Writer.
func ProtoEncode(msg ProtoMessage, buf []byte, writer io.Writer) error {
	protoSizeBytes := msg.Size()
	// TODO(bodu): Handle case where data length doesn't fit into 4 bytes.
	endianness.PutUint32(buf, uint32(protoSizeBytes))
	if _, err := writer.Write(buf[:Uint32SizeBytes]); err != nil {
		return err
	}
	if _, err := msg.MarshalTo(buf); err != nil {
		return err
	}
	_, err := writer.Write(buf[:protoSizeBytes])
	return err
}

// ProtoDecode a proto message to an io.Writer.
func ProtoDecode(msg ProtoMessage, buf []byte, reader io.Reader) error {
	if _, err := reader.Read(buf); err != nil {
		return err
	}

	return msg.Unmarshal(buf)
}

// MinNumberOfBits returns the minimum # of bits required to store a max value.
func MinNumberOfBits(maxValue int) float64 {
	return math.Max(float64(1), 64-float64(bits.LeadingZeros(uint(maxValue))))
}
