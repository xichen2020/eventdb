package document

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pilosa/pilosa/roaring"
)

// DocIDSet is a document ID set.
type DocIDSet interface {
	// NumDocuments returns the total number of documents in the set.
	NumDocuments() int32

	// Iter returns the document ID set iterator.
	Iter() DocIDSetIterator

	// WriteTo writes the document ID set to an io.Writer.
	// NB: extBuf is an external buffer for reuse.
	WriteTo(writer io.Writer, extBuf *bytes.Buffer) error
}

// DocIDSetIterator is the document ID set iterator.
type DocIDSetIterator interface {
	// Next returns true if there are more document IDs to be iterated over.
	Next() bool

	// DocID returns the current document ID.
	// NB: This is not called `Current` because it needs to
	// be embedded with other iterators so the method name is
	// more specific w.r.t. what value this is referring to.
	DocID() int32

	// Close closes the iterator.
	Close()
}

// docIDSetBuilder builds a document ID set.
type docIDSetBuilder interface {
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

// fullDocIDSet is a doc ID set that is known to be full.
type fullDocIDSet struct {
	numTotalDocs int32
}

func (s *fullDocIDSet) NumDocuments() int32 { return s.numTotalDocs }

func (s *fullDocIDSet) Iter() DocIDSetIterator { return NewFullDocIDSetIterator(s.numTotalDocs) }

func (s *fullDocIDSet) WriteTo(writer io.Writer, _ *bytes.Buffer) error {
	// Write Doc ID set type.
	if err := writeVarint(writer, int32(fullDocIDSetType)); err != nil {
		return err
	}
	return writeVarint(writer, s.numTotalDocs)
}

// TODO(xichen): Perhaps pool the the roaring bitmaps.
type bitmapBasedDocIDSetBuilder struct {
	bm *roaring.Bitmap
}

func newBitmapBasedDocIDSetBuilder(bm *roaring.Bitmap) *bitmapBasedDocIDSetBuilder {
	return &bitmapBasedDocIDSetBuilder{bm: bm}
}

func (s *bitmapBasedDocIDSetBuilder) Add(docID int32) { s.bm.DirectAdd(uint64(docID)) }

// NB(xichen): Clone the internal bitmap so the builder can be mutated independently
// of the snapshot. In the future we can look into the bitmap implementation to see
// if there are more efficient ways of doing this without requiring a full copy.
func (s *bitmapBasedDocIDSetBuilder) Snapshot() DocIDSet {
	return &bitmapBasedDocIDSet{bm: s.bm.Clone()}
}

func (s *bitmapBasedDocIDSetBuilder) Seal(numTotalDocs int32) DocIDSet {
	if int(s.bm.Count()) == int(numTotalDocs) {
		// This is a full doc ID set, so we use a more efficient representation.
		s.bm = nil
		return &fullDocIDSet{numTotalDocs: numTotalDocs}
	}
	// TODO(xichen): Investigate the impact of bitmap optimization.
	s.bm.Optimize()
	res := &bitmapBasedDocIDSet{
		numTotalDocs: numTotalDocs,
		bm:           s.bm,
	}
	s.bm = nil
	return res
}

type bitmapBasedDocIDSet struct {
	numTotalDocs int32
	bm           *roaring.Bitmap
}

func (s *bitmapBasedDocIDSet) NumDocuments() int32 { return s.numTotalDocs }

func (s *bitmapBasedDocIDSet) Iter() DocIDSetIterator {
	return newbitmapBasedDocIDIterator(s.bm.Iterator())
}

func (s *bitmapBasedDocIDSet) WriteTo(writer io.Writer, extBuf *bytes.Buffer) error {
	// Write Doc ID set type.
	if err := writeVarint(writer, int32(bitmapBasedDocIDSetType)); err != nil {
		return err
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

type bitmapBasedDocIDIterator struct {
	rit *roaring.Iterator

	closed bool
	done   bool
	curr   int32
}

func newbitmapBasedDocIDIterator(rit *roaring.Iterator) *bitmapBasedDocIDIterator {
	return &bitmapBasedDocIDIterator{rit: rit}
}

func (it *bitmapBasedDocIDIterator) Next() bool {
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

func (it *bitmapBasedDocIDIterator) DocID() int32 { return it.curr }

func (it *bitmapBasedDocIDIterator) Close() {
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

// DocIDSetIteratorFn transforms an input doc ID set iterator into a new doc ID set iterator.
type DocIDSetIteratorFn func(it DocIDSetIterator) DocIDSetIterator

// NoOpDocIDSetIteratorFn is a no op transformation function that returns the input iterator as is.
func NoOpDocIDSetIteratorFn(it DocIDSetIterator) DocIDSetIterator { return it }

// ExcludeDocIDSetIteratorFn returns a transformation function that excludes the doc ID set
// associated with the input iterator from the full doc ID set.
func ExcludeDocIDSetIteratorFn(numTotalDocs int32) DocIDSetIteratorFn {
	return func(it DocIDSetIterator) DocIDSetIterator {
		return NewExcludeDocIDSetIterator(numTotalDocs, it)
	}
}
