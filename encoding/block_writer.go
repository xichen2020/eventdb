package encoding

import (
	"bytes"
	"io"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/proto"
)

// blockWriter encapsulates
type blockWriter struct {
	w         io.Writer
	buf       *bytes.Buffer
	extBuf    *[]byte
	metaProto *encodingpb.BlockMeta
}

func newBlockWriter(
	// extBuf is a shared buffer btwn the block writer and the encoder for memory re-use.
	extBuf *[]byte,
	// extProto is shared btwn the block writer and the encoder and allows the encoder
	// to inform the block writer of how many events have been written.
	extProto *encodingpb.BlockMeta,
) *blockWriter {
	return &blockWriter{
		buf:       bytes.NewBuffer(nil),
		extBuf:    extBuf,
		metaProto: extProto,
	}
}

func (bw *blockWriter) Close() error {
	bw.buf = nil
	bw.extBuf = nil
	bw.w = nil
	bw.metaProto = nil
	return nil
}

func (bw *blockWriter) reset(writer io.Writer) {
	bw.buf.Reset()
	bw.metaProto.Reset()
	if writer != nil {
		bw.w = writer
	}
}

func (bw *blockWriter) flush() error {
	defer bw.reset(nil)

	bw.metaProto.NumBytes = int64(bw.buf.Len())
	// Write block metadata out first.
	if err := proto.EncodeBlockMeta(bw.metaProto, bw.extBuf, bw.w); err != nil {
		return err
	}

	// Write out compressed data next.
	_, err := bw.w.Write(bw.buf.Bytes())
	return err
}
