package decoding

import (
	"bytes"
	"io"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	xproto "github.com/xichen2020/eventdb/x/proto"
)

const (
	trueByte = 1
)

// BoolDecoder decodes bool values.
type BoolDecoder interface {
	// DecodeRaw decodes raw encoded bytes into a closeable bool values.
	DecodeRaw(data []byte) (values.CloseableBoolValues, error)
}

// BoolDec is a bool decoder.
type BoolDec struct{}

// NewBoolDecoder creates a new bool decoder.
func NewBoolDecoder() *BoolDec { return &BoolDec{} }

// DecodeRaw decodes raw encoded bytes into a closeable bool values.
func (dec *BoolDec) DecodeRaw(data []byte) (values.CloseableBoolValues, error) {
	var metaProto encodingpb.BoolMeta
	bytesRead, err := xproto.DecodeBoolMetaRaw(data, &metaProto)
	if err != nil {
		return nil, err
	}
	return newFsBasedBoolValues(metaProto, data[bytesRead:]), nil
}

// NB(xichen): This can be made more efficient by directly passing in a byte slice
// when creating the iterator.
func newBoolIteratorFromMeta(
	_ encodingpb.BoolMeta,
	encodedBytes []byte,
) (iterator.ForwardBoolIterator, error) {
	reader := bytes.NewReader(encodedBytes)
	return runLengthDecodeBool(readBool, reader), nil
}

func readBool(reader io.ByteReader) (bool, error) {
	var value bool
	b, err := reader.ReadByte()
	if err != nil {
		return value, err
	}
	if b == trueByte {
		value = true
	}
	return value, nil
}
