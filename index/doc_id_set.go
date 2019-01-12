package index

import (
	"bytes"
	"fmt"
	"io"

	xio "github.com/xichen2020/eventdb/x/io"

	"github.com/pilosa/pilosa/roaring"
)

// DocIDSet is a document ID set.
type DocIDSet interface {
	// Iter returns the document ID set iterator.
	Iter() DocIDSetIterator

	// Intersect returns the set of positions in the current doc ID set that
	// are also in the doc ID set given by the iterator passed in.
	Intersect(it DocIDSetIterator) DocIDPositionIterator

	// WriteTo writes the document ID set to an io.Writer.
	// NB: extBuf is an external buffer for reuse.
	WriteTo(writer io.Writer, extBuf *bytes.Buffer) error
}

type unmarshallableDocIDSet interface {
	DocIDSet

	// readFrom reads the document ID set from a byte slice, returning the
	// number of bytes read and any error encountered. Note that the given
	// buffer does not have the doc ID set type encoded.
	readFrom(buf []byte) (int, error)
}

// DocIDSetBuilder builds a document ID set.
type DocIDSetBuilder interface {
	// Add adds a single document ID.
	Add(docID int32)

	// Snapshot returns an immutable snapshot of the builder state.
	// Snapshot can be processed independently of operations performed against the builder.
	Snapshot() DocIDSet

	// Seal seals a doc ID set effectively making it immutable.
	Seal(numTotalDocs int32) DocIDSet
}

type docIDSetType int

const (
	fullDocIDSetType docIDSetType = iota
	bitmapBasedDocIDSetType
)

// NewDocIDSetFromBytes creates a new doc ID set from raw bytes, returning the newly
// created doc ID set and number of bytes read. Otherwise, an error is returned.
func NewDocIDSetFromBytes(data []byte) (DocIDSet, int, error) {
	typeID, bytesRead, err := xio.ReadVarint(data)
	if err != nil {
		return nil, 0, err
	}
	disType := docIDSetType(typeID)

	var dis unmarshallableDocIDSet
	switch disType {
	case fullDocIDSetType:
		dis = newFullDocIDSet(0)
	case bitmapBasedDocIDSetType:
		dis = newBitmapBasedDocIDSet(roaring.NewBitmap())
	default:
		return nil, 0, fmt.Errorf("unknown doc ID set type: %v", disType)
	}

	n, err := dis.readFrom(data[bytesRead:])
	if err != nil {
		return nil, 0, err
	}
	bytesRead += n
	return dis, bytesRead, nil
}

// fullDocIDSet is a doc ID set that is known to be full.
type fullDocIDSet struct {
	numTotalDocs int32
}

func newFullDocIDSet(numTotalDocs int32) *fullDocIDSet {
	return &fullDocIDSet{numTotalDocs: numTotalDocs}
}

func (s *fullDocIDSet) Iter() DocIDSetIterator { return NewFullDocIDSetIterator(s.numTotalDocs) }

func (s *fullDocIDSet) Intersect(it DocIDSetIterator) DocIDPositionIterator {
	return newFullDocIDPositionIterator(s.numTotalDocs, it)
}

func (s *fullDocIDSet) WriteTo(writer io.Writer, _ *bytes.Buffer) error {
	// Write Doc ID set type.
	if err := xio.WriteVarint(writer, int64(fullDocIDSetType)); err != nil {
		return err
	}
	return xio.WriteVarint(writer, int64(s.numTotalDocs))
}

// NB: The given buffer does not have the doc ID set type encoded.
func (s *fullDocIDSet) readFrom(buf []byte) (int, error) {
	numTotalDocs, bytesRead, err := xio.ReadVarint(buf)
	if err != nil {
		return 0, err
	}
	s.numTotalDocs = int32(numTotalDocs)
	return bytesRead, nil
}

// TODO(xichen): Perhaps pool the the roaring bitmaps.
type bitmapBasedDocIDSetBuilder struct {
	bm *roaring.Bitmap
}

// NewBitmapBasedDocIDSetBuilder creates a new bitmap based doc ID set builder.
func NewBitmapBasedDocIDSetBuilder(bm *roaring.Bitmap) DocIDSetBuilder {
	return &bitmapBasedDocIDSetBuilder{bm: bm}
}

func (s *bitmapBasedDocIDSetBuilder) Add(docID int32) { s.bm.DirectAdd(uint64(docID)) }

// NB(xichen): Clone the internal bitmap so the builder can be mutated independently
// of the snapshot. In the future we can look into the bitmap implementation to see
// if there are more efficient ways of doing this without requiring a full copy.
func (s *bitmapBasedDocIDSetBuilder) Snapshot() DocIDSet {
	return newBitmapBasedDocIDSet(s.bm.Clone())
}

func (s *bitmapBasedDocIDSetBuilder) Seal(numTotalDocs int32) DocIDSet {
	if int(s.bm.Count()) == int(numTotalDocs) {
		// This is a full doc ID set, so we use a more efficient representation.
		s.bm = nil
		return newFullDocIDSet(numTotalDocs)
	}
	// TODO(xichen): Investigate the impact of bitmap optimization.
	s.bm.Optimize()
	res := newBitmapBasedDocIDSet(s.bm)
	s.bm = nil
	return res
}

type bitmapBasedDocIDSet struct {
	bm *roaring.Bitmap
}

func newBitmapBasedDocIDSet(bm *roaring.Bitmap) *bitmapBasedDocIDSet {
	return &bitmapBasedDocIDSet{bm: bm}
}

func (s *bitmapBasedDocIDSet) Iter() DocIDSetIterator {
	return newBitmapBasedDocIDIterator(s.bm.Iterator())
}

func (s *bitmapBasedDocIDSet) Intersect(it DocIDSetIterator) DocIDPositionIterator {
	return NewDocIDPositionIterator(s.Iter(), it)
}

func (s *bitmapBasedDocIDSet) WriteTo(writer io.Writer, extBuf *bytes.Buffer) error {
	// Write Doc ID set type.
	if err := xio.WriteVarint(writer, int64(bitmapBasedDocIDSetType)); err != nil {
		return err
	}

	// TODO(xichen): Precompute the size of the encoded bitmap to avoid the memory
	// cost of writing the encoded bitmap to a byte buffer then to file.
	extBuf.Reset()
	_, err := s.bm.WriteTo(extBuf)
	if err != nil {
		return err
	}
	if err = xio.WriteVarint(writer, int64(extBuf.Len())); err != nil {
		return err
	}
	_, err = writer.Write(extBuf.Bytes())
	return err
}

// NB: The given buffer does not have the doc ID set type encoded.
func (s *bitmapBasedDocIDSet) readFrom(buf []byte) (int, error) {
	size, bytesRead, err := xio.ReadVarint(buf)
	if err != nil {
		return 0, err
	}
	encodeEnd := bytesRead + int(size)
	if encodeEnd > len(buf) {
		return 0, fmt.Errorf("bitmap based doc ID set size %d exceeds available buffer size %d", size, len(buf)-bytesRead)
	}
	encoded := buf[bytesRead:encodeEnd]
	if err := s.bm.UnmarshalBinary(encoded); err != nil {
		return 0, err
	}
	bytesRead = encodeEnd
	return bytesRead, nil
}
