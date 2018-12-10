package encoding

import (
	"encoding/binary"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"
)

// DictionaryBasedStringIterator iterates over a
// dict encoded stream of string data.
type DictionaryBasedStringIterator struct {
	reader io.Reader
	// dictionary is passed externally from the string encoder
	// and should not be mutated during iteration.
	dictionary []string
	curr       string
	err        error
	closed     bool
}

func newDictionaryBasedStringIterator(
	reader io.Reader,
	extProto *encodingpb.StringArray, // extProto is an external proto for memory re-use.
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
) (*DictionaryBasedStringIterator, error) {
	if err := proto.DecodeStringArray(extProto, extBuf, reader); err != nil {
		return nil, err
	}
	return &DictionaryBasedStringIterator{
		reader:     reader,
		dictionary: extProto.Data,
	}, nil
}

// Next iteration.
func (d *DictionaryBasedStringIterator) Next() bool {
	if d.closed || d.err != nil {
		return false
	}

	var idx int64
	idx, d.err = binary.ReadVarint(d.reader)
	if d.err != nil {
		return false
	}

	d.curr = d.dictionary[idx]
	return true
}

// Current returns the current string.
func (d *DictionaryBasedStringIterator) Current() string { return d.curr }

// Err returns any error recorded while iterating.
func (d *DictionaryBasedStringIterator) Err() error { return d.err }

// Close the iterator.
func (d *DictionaryBasedStringIterator) Close() error {
	d.closed = true
	d.dictionary = nil
	return nil
}
