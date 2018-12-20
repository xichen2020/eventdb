package document

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pilosa/pilosa/roaring"
)

// DocIDSet is a document ID set.
type DocIDSet interface {
	// IsFull returns true if the set contains all documents for a given number of docs.
	IsFull() bool

	// NumDocuments returns the total number of documents in the set.
	NumDocuments() int32

	// Iter returns the document ID set iterator.
	Iter() DocIDSetIterator

	// WriteTo writes the document ID set to an io.Writer.
	// NB: extBuf is an external buffer for reuse.
	WriteTo(writer io.Writer, extBuf *bytes.Buffer) error
}

// DocIDSetBuilder builds a document ID set.
type DocIDSetBuilder interface {
	// Add adds a single document ID.
	Add(docID int32)

	// Build returns a readonly doc ID set.
	Build(numTotalDocs int) DocIDSet
}

// DocIDSetIterator is the document ID set iterator.
type DocIDSetIterator interface {
	// Next returns true if there are more document IDs to be iterated over.
	Next() bool

	// Current returns the current document ID.
	Current() int32

	// Close closes the iterator.
	Close()
}

type fullDocIDSetBuilder struct{}

func (b fullDocIDSetBuilder) Add(docID int32) {}

func (b fullDocIDSetBuilder) Build(numTotalDocs int) DocIDSet {
	return &fullDocIDSet{numTotalDocs: numTotalDocs}
}

// fullDocIDSet is a doc ID set that is known to be full.
type fullDocIDSet struct {
	numTotalDocs int
}

func (s *fullDocIDSet) IsFull() bool { return true }

func (s *fullDocIDSet) NumDocuments() int32 { return int32(s.numTotalDocs) }

func (s *fullDocIDSet) Iter() DocIDSetIterator { return newFullDocIDSetIter(s.numTotalDocs) }

func (s *fullDocIDSet) WriteTo(writer io.Writer, _ *bytes.Buffer) error {
	return writeVarint(writer, int32(s.numTotalDocs))
}

// fullDocIDSetIter is an iterator for a full doc ID set containing document IDs
// ranging from 0 (inclusive) to `numTotalDocs` (exclusive).
type fullDocIDSetIter struct {
	numTotalDocs int

	curr int
}

func newFullDocIDSetIter(numTotalDocs int) *fullDocIDSetIter {
	return &fullDocIDSetIter{numTotalDocs: numTotalDocs, curr: -1}
}

func (it *fullDocIDSetIter) Next() bool {
	if it.curr >= it.numTotalDocs {
		return false
	}
	it.curr++
	return it.curr < it.numTotalDocs
}

func (it *fullDocIDSetIter) Current() int32 { return int32(it.curr) }
func (it *fullDocIDSetIter) Close()         {}

// TODO(xichen): Perhaps pool the the roaring bitmaps.
type bitmapBasedDocIDSetBuilder struct {
	bm *roaring.Bitmap
}

func newBitmapBasedDocIDSetBuilder(bm *roaring.Bitmap) *bitmapBasedDocIDSetBuilder {
	return &bitmapBasedDocIDSetBuilder{bm: bm}
}

func (s *bitmapBasedDocIDSetBuilder) Add(docID int32) { s.bm.Add(uint64(docID)) }

func (s *bitmapBasedDocIDSetBuilder) Build(numTotalDocs int) DocIDSet {
	return &bitmapBasedDocIDSet{
		numTotalDocs: numTotalDocs,
		bm:           s.bm,
	}
}

type bitmapBasedDocIDSet struct {
	numTotalDocs int
	bm           *roaring.Bitmap
}

func (s *bitmapBasedDocIDSet) IsFull() bool { return s.numTotalDocs == int(s.bm.Count()) }

func (s *bitmapBasedDocIDSet) NumDocuments() int32 { return int32(s.numTotalDocs) }

func (s *bitmapBasedDocIDSet) Iter() DocIDSetIterator {
	return newbitmapBasedDocIDIter(s.bm.Iterator())
}

func (s *bitmapBasedDocIDSet) WriteTo(writer io.Writer, extBuf *bytes.Buffer) error {
	// Fast, more efficient path for writing full doc ID set.
	if s.IsFull() {
		ds := fullDocIDSet{numTotalDocs: s.numTotalDocs}
		return ds.WriteTo(writer, extBuf)
	}

	// TODO(xichen): Precompute the size of the encoded bitmap to avoid the memory
	// cost of writing the encoded bitmap to a byte buffer then to file.
	extBuf.Reset()
	_, err := s.bm.WriteTo(extBuf)
	if err != nil {
		return err
	}
	if err = writeVarint(writer, int32(extBuf.Len())); err != nil {
		return err
	}
	_, err = writer.Write(extBuf.Bytes())
	return err
}

type bitmapBasedDocIDIter struct {
	rit *roaring.Iterator

	closed bool
	done   bool
	curr   int32
}

func newbitmapBasedDocIDIter(rit *roaring.Iterator) *bitmapBasedDocIDIter {
	return &bitmapBasedDocIDIter{rit: rit}
}

func (it *bitmapBasedDocIDIter) Next() bool {
	if it.done || it.closed {
		return false
	}
	curr, eof := it.rit.Next()
	if eof {
		it.done = true
		return false
	}
	it.curr = int32(curr)
	return true
}

func (it *bitmapBasedDocIDIter) Current() int32 { return it.curr }

func (it *bitmapBasedDocIDIter) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.rit = nil
}

func writeVarint(writer io.Writer, v int32) error {
	var buf [binary.MaxVarintLen32]byte
	size := binary.PutVarint(buf[:], int64(v))
	_, err := writer.Write(buf[:size])
	return err
}
