package template

import (
	"encoding/binary"
	"io"

	"github.com/mauricelam/genny/generic"
	"github.com/xichen2020/eventdb/x/bytes"
)

// GenericProtoMessage is a generic gogo protobuf generated Message type.
type GenericProtoMessage interface {
	generic.Type

	MarshalTo(dst []byte) (int, error)
	Size() int
}

// EncodeValue encodes a GenericValue message into a `io.Writer`.
// pool if the array is at capacity.
func EncodeValue(
	msg GenericProtoMessage,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
	writer io.Writer,
) error {
	protoSizeBytes := msg.Size()
	*extBuf = bytes.EnsureBufferSize(*extBuf, int(protoSizeBytes), bytes.DontCopyData)
	n := binary.PutVarint(*extBuf, int64(protoSizeBytes))
	if _, err := writer.Write((*extBuf)[:n]); err != nil {
		return err
	}
	if _, err := msg.MarshalTo((*extBuf)[:protoSizeBytes]); err != nil {
		return err
	}
	_, err := writer.Write((*extBuf)[:protoSizeBytes])
	return err
}
