package template

import (
	"encoding/binary"
	"io"

	"github.com/mauricelam/genny/generic"
	"github.com/xichen2020/eventdb/x/bytes"
	xio "github.com/xichen2020/eventdb/x/io"
)

// GenericDecodeProtoMessage is a generic gogo protobuf generated Message type.
type GenericDecodeProtoMessage interface {
	generic.Type

	Unmarshal(dst []byte) error
}

// DecodeValue decodes a GenericDecodeProtoMessage message into a `io.Writer`.
func DecodeValue(
	msg GenericDecodeProtoMessage,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
	reader xio.Reader,
) error {
	protoSizeBytes, err := binary.ReadVarint(reader)
	if err != nil {
		return err
	}
	*extBuf = bytes.EnsureBufferSize(*extBuf, int(protoSizeBytes), bytes.DontCopyData)
	if _, err := io.ReadFull(reader, (*extBuf)[:protoSizeBytes]); err != nil {
		return err
	}
	return msg.Unmarshal((*extBuf)[:protoSizeBytes])
}
