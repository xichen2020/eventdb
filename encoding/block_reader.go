package encoding

import (
	"errors"
	"os"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"
)

var (
	errEndOfBlock = errors.New("reached end of block, call Skip to move on to the next block")
)

type blockReader struct {
	// Two readers, one for reading data. And another for seeking through data.
	r         io.SeekableReader
	extBuf    *[]byte
	metaProto *encodingpb.BlockMeta
	// numBytesRead is used to determine how many bytes ahead to skip for the
	// next block.
	numBytesRead int64
}

func newBlockReader(
	// extBuf is a shared buffer btwn the block writer and the decoder for memory re-use.
	extBuf *[]byte,
	// extProto is a shared proto btwn the block writer and the decoder for memory re-use.
	extProto *encodingpb.BlockMeta,
) *blockReader {
	return &blockReader{
		extBuf:    extBuf,
		metaProto: extProto,
	}
}

func (br *blockReader) Read(p []byte) (int, error) {
	// Return an end of block error if the reader has reached the end of a block.
	if br.metaProto.NumBytes == br.numBytesRead {
		return 0, errEndOfBlock
	}

	// Calculate # of bytes to read so we don't overshoot.
	bytesToRead := int64(len(p))
	remainingBytes := br.metaProto.NumBytes - br.numBytesRead
	if bytesToRead > remainingBytes {
		bytesToRead = remainingBytes
	}

	n, err := br.r.Read(p[:bytesToRead])
	if err != nil {
		return 0, err
	}
	br.numBytesRead += int64(n)

	return n, nil
}

func (br *blockReader) ReadByte() (byte, error) {
	n, err := br.Read((*br.extBuf)[:1])
	if err != nil {
		return 0, err
	}
	br.numBytesRead += int64(n)
	return (*br.extBuf)[0], nil
}

// skip to the next block.
// NB(bodu): it is the caller's responsibility to call skip when they reach the end of a block.
func (br *blockReader) skip() error {
	// Seek to the next block in the underlying reader.
	if br.metaProto.NumBytes > 0 {
		if _, err := br.r.Seek(br.metaProto.NumBytes-br.numBytesRead, os.SEEK_CUR); err != nil {
			return err
		}
	}

	br.metaProto.Reset()
	if err := proto.DecodeBlockMeta(br.metaProto, br.extBuf, br.r); err != nil {
		return err
	}
	br.numBytesRead = 0
	return nil
}

// numOfEvents is used by the caller to determine whether or not to skip.
func (br *blockReader) numOfEvents() int {
	return int(br.metaProto.NumOfEvents)
}

func (br *blockReader) reset(reader io.SeekableReader) {
	br.metaProto.Reset()
	br.r = reader
}
