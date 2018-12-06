package encoding

import (
	"encoding/binary"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/bytes"
)

type dictionaryBasedStringIterator struct {
	reader     Reader
	dictionary []string
	buf        []byte
	curr       string
	err        error
	closed     bool
	zero       uint64
}

func newDictionaryBasedStringIterator(
	reader Reader,
	dictionaryProto *encodingpb.StringArray,
	bufRef *[]byte,
) (*dictionaryBasedStringIterator, error) {
	// Passed in a buf ref to manipulate the decoder buf w/o a ref to the decoder.
	buf := *bufRef
	if _, err := reader.Read(buf[:Uint32SizeBytes]); err != nil {
		return nil, err
	}
	protoSizeBytes := int(endianness.Uint32(buf[:Uint32SizeBytes]))
	*bufRef = bytes.EnsureBufferSize(buf, protoSizeBytes, bytes.DontCopyData)
	buf = *bufRef
	if err := ProtoDecode(dictionaryProto, buf[:protoSizeBytes], reader); err != nil {
		return nil, err
	}
	return &dictionaryBasedStringIterator{
		reader:     reader,
		dictionary: dictionaryProto.Data,
		buf:        make([]byte, Uint64SizeBytes),
	}, nil
}

func (d *dictionaryBasedStringIterator) Next() bool {
	if d.closed || d.err != nil {
		return false
	}

	// Zero out the byte buffer before each iteration.
	endianness.PutUint64(d.buf, d.zero)

	var idx int64
	idx, d.err = binary.ReadVarint(d.reader)
	if d.err != nil {
		return false
	}

	d.curr = d.dictionary[idx]

	return true
}

func (d *dictionaryBasedStringIterator) Current() string {
	return d.curr
}

func (d *dictionaryBasedStringIterator) Err() error {
	return d.err
}

// Reset of internal data not supported for dictionary iterator.
func (d *dictionaryBasedStringIterator) Reset(values []string) {}

func (d *dictionaryBasedStringIterator) Close() error {
	d.closed = true
	return nil
}
