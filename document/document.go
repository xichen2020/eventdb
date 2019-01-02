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
