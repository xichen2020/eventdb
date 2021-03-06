// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package proto

import (
	"encoding/binary"

	"fmt"

	"io"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/bytes"

	xio "github.com/xichen2020/eventdb/x/io"
)

// *encodingpb.IntMeta is a generic gogo protobuf generated Message type.

// DecodeIntMeta decodes a *encodingpb.IntMeta message from a reader.
func DecodeIntMeta(
	reader xio.Reader,
	msg *encodingpb.IntMeta,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
) (int, error) {
	protoSizeBytes, err := binary.ReadVarint(reader)
	if err != nil {
		return 0, err
	}
	// Compute the number of bytes read from the reader for the proto size bytes.
	bytesRead := xio.VarintBytes(protoSizeBytes)

	var buf []byte
	if extBuf == nil {
		buf = make([]byte, protoSizeBytes)
	} else {
		*extBuf = bytes.EnsureBufferSize(*extBuf, int(protoSizeBytes), bytes.DontCopyData)
		buf = *extBuf
	}
	if _, err := io.ReadFull(reader, buf[:protoSizeBytes]); err != nil {
		return 0, err
	}
	if err := msg.Unmarshal(buf[:protoSizeBytes]); err != nil {
		return 0, err
	}
	bytesRead += int(protoSizeBytes)
	return bytesRead, nil
}

// DecodeIntMetaRaw decodes raw bytes into a protobuf message, returning the
// number of bytes read and any errors encountered.
func DecodeIntMetaRaw(
	data []byte,
	msg *encodingpb.IntMeta,
) (int, error) {
	protoSizeBytes, bytesRead, err := xio.ReadVarint(data)
	if err != nil {
		return 0, err
	}
	remainder := data[bytesRead:]
	if int(protoSizeBytes) > len(remainder) {
		return 0, fmt.Errorf("decoded message size %d exceeds available buffer size %d", protoSizeBytes, len(remainder))
	}
	marshalledBytes := remainder[:protoSizeBytes]
	if err := msg.Unmarshal(marshalledBytes); err != nil {
		return 0, err
	}
	bytesRead += int(protoSizeBytes)
	return bytesRead, nil
}
