package convert

import (
	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
)

// ToDocuments converts a list of documents represented in protobuf to
// a list of documents in internal representation.
func ToDocuments(
	pbDocs []servicepb.Document,
	documentArrayPool *document.BucketizedDocumentArrayPool,
	fieldArrayPool *field.BucketizedFieldArrayPool,
) ([]document.Document, error) {
	docs := documentArrayPool.Get(len(pbDocs))[:0]
	for _, pbDoc := range pbDocs {
		doc, err := ToDocument(pbDoc, fieldArrayPool)
		if err != nil {
			document.ReturnArrayToPool(docs, documentArrayPool)
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// ToDocument converts a document represented in protobuf to an internal
// document representation.
func ToDocument(
	pbDoc servicepb.Document,
	fieldArrayPool *field.BucketizedFieldArrayPool,
) (document.Document, error) {
	fields, err := ToFields(pbDoc.Fields, fieldArrayPool)
	if err != nil {
		return document.Document{}, err
	}
	return document.Document{
		ID:             pbDoc.Id,
		TimeNanos:      pbDoc.TimeNanos,
		RawData:        pbDoc.RawData,
		Fields:         fields,
		FieldArrayPool: fieldArrayPool,
	}, nil
}

// ToFields converts a list of fields represented in protobuf to a list of
// fields in internal representations.
func ToFields(
	pbFields []servicepb.Field,
	fieldArrayPool *field.BucketizedFieldArrayPool,
) ([]field.Field, error) {
	fields := fieldArrayPool.Get(len(pbFields))[:0]
	for _, pbField := range pbFields {
		f, err := field.NewFieldFromProto(pbField)
		if err != nil {
			field.ReturnArrayToPool(fields, fieldArrayPool)
			return nil, err
		}
		fields = append(fields, f)
	}
	return fields, nil
}
