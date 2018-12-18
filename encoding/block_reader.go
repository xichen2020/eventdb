package encoding

import (
	"os"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"
)

type blockReader struct {
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
	// Skip to the next block if we've read this entire block of data.
	if br.metaProto.NumBytes == br.numBytesRead {
		if err := br.skip(); err != nil {
			return 0, err
		}
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

	// Skip to the next block if we've read this entire block of data.
	if br.metaProto.NumBytes == br.numBytesRead {
		if err := br.skip(); err != nil {
			return 0, err
		}
	}
	return (*br.extBuf)[0], nil
}

// Skip to the next block. Also called on `reset` to load the first block metadata.
func (br *blockReader) skip() error {
	br.metaProto.Reset()

	// Seek to the next block in the underlying reader.
	if _, err := br.r.Seek(br.metaProto.NumBytes-br.numBytesRead, os.SEEK_CUR); err != nil {
		return err
	}
	if err := proto.DecodeBlockMeta(br.metaProto, br.extBuf, br.r); err != nil {
		return err
	}
	return nil
}

func (br *blockReader) reset(reader io.SeekableReader) {
	br.metaProto.Reset()
	br.r = reader
}

// numOfEvents is used by the caller to determine whether or not to skip.
func (br *blockReader) numOfEvents() int {
	return int(br.metaProto.NumOfEvents)
}
