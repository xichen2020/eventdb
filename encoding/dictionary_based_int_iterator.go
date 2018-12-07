package encoding

import (
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"

	bitstream "github.com/dgryski/go-bitstream"
)

// DictionaryBasedIntIterator iterates through a dict encoded stream of ints.
type DictionaryBasedIntIterator struct {
	bitReader               *bitstream.BitReader
	bytesPerDictionaryValue int64
	bitsPerEncodedValue     int64
	extBuf                  *[]byte
	dict                    []byte
	curr                    int
	err                     error
	closed                  bool
}

// NewDictionaryBasedIntIterator returns a new dictionary based int iterator.
func NewDictionaryBasedIntIterator(
	reader io.Reader,
	extProto *encodingpb.IntDictionary, // extProto is an external proto for memory re-use.
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
	bytesPerDictionaryValue int64,
	bitsPerEncodedValue int64,
) (*DictionaryBasedIntIterator, error) {
	if err := proto.DecodeIntDictionary(extProto, extBuf, reader); err != nil {
		return nil, err
	}
	// Zero out extBuf so we can re-use it during iteration.
	endianness.PutUint64(*extBuf, uint64(0))
	return &DictionaryBasedIntIterator{
		bitReader:               bitstream.NewReader(reader),
		bytesPerDictionaryValue: bytesPerDictionaryValue,
		bitsPerEncodedValue:     bitsPerEncodedValue,
		extBuf:                  extBuf,
		dict:                    extProto.Data,
	}, nil
}

// Next iteration.
func (d *DictionaryBasedIntIterator) Next() bool {
	if d.closed || d.err != nil {
		return false
	}

	// Read the idx into the dict first.
	var dictIdx uint64
	dictIdx, d.err = d.bitReader.ReadBits(int(d.bitsPerEncodedValue))
	if d.err != nil {
		return false
	}

	// Use idx to fetch value.
	start := int64(dictIdx) * d.bytesPerDictionaryValue
	copy((*d.extBuf)[:d.bytesPerDictionaryValue], d.dict[start:start+d.bytesPerDictionaryValue])
	d.curr = int(endianness.Uint64(*d.extBuf))
	return true
}

// Current returns the current int.
func (d *DictionaryBasedIntIterator) Current() int {
	return d.curr
}

// Err returns any error recorded while iterating.
func (d *DictionaryBasedIntIterator) Err() error {
	return d.err
}

// Close the iterator.
func (d *DictionaryBasedIntIterator) Close() error {
	d.closed = true
	return nil
}
