package document

import (
	"github.com/xichen2020/eventdb/document/field"
)

// Document is a discrete, timestamped document.
type Document struct {
	ID        []byte
	TimeNanos int64
	FieldIter field.Iterator
	RawData   []byte
}

// Reset resets a document.
func (d *Document) Reset() {
	d.ID = nil
	d.RawData = nil
	if d.FieldIter != nil {
		d.FieldIter.Close()
		d.FieldIter = nil
	}
}

// ReturnArrayToPool returns a document array to pool.
func ReturnArrayToPool(docs []Document, p *BucketizedDocumentArrayPool) {
	if p == nil {
		return
	}
	for i := 0; i < len(docs); i++ {
		docs[i].Reset()
	}
	docs = docs[:0]
	p.Put(docs, cap(docs))
}
