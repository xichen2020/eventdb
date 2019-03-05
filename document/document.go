package document

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
)

// Document is a discrete, timestamped document.
type Document struct {
	ID             []byte
	TimeNanos      int64
	RawData        []byte
	Fields         field.Fields
	FieldArrayPool *field.BucketizedFieldArrayPool
}

// Reset resets a document.
func (d *Document) Reset() {
	d.ID = nil
	d.RawData = nil
	if d.Fields != nil {
		field.ReturnArrayToPool(d.Fields, d.FieldArrayPool)
		d.Fields = nil
	}
}

// ToProto converts a document to a document proto message.
func (d *Document) ToProto() (servicepb.Document, error) {
	pbFields, err := d.Fields.ToProto()
	if err != nil {
		return servicepb.Document{}, err
	}
	return servicepb.Document{
		Id:        d.ID,
		TimeNanos: d.TimeNanos,
		RawData:   d.RawData,
		Fields:    pbFields,
	}, nil
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

// Documents is a list of documents.
type Documents []Document

// ToProto converts a list of documents to a documents protobuf message.
func (docs Documents) ToProto() ([]servicepb.Document, error) {
	res := make([]servicepb.Document, 0, len(docs))
	for _, doc := range docs {
		pbDoc, err := doc.ToProto()
		if err != nil {
			return nil, err
		}
		res = append(res, pbDoc)
	}
	return res, nil
}
