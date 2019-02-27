package convert

import (
	"fmt"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/x/safe"
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
		ID:        pbDoc.Id,
		TimeNanos: pbDoc.TimeNanos,
		FieldIter: field.NewArrayBasedIterator(fields, fieldArrayPool),
		RawData:   pbDoc.RawData,
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
		f, err := ToField(pbField)
		if err != nil {
			field.ReturnArrayToPool(fields, fieldArrayPool)
			return nil, err
		}
		fields = append(fields, f)
	}
	return fields, nil
}

// ToField converts a field represented in protobuf to a field in internal representations.
func ToField(pbField servicepb.Field) (field.Field, error) {
	value, err := ToFieldValue(pbField.Value)
	if err != nil {
		return field.Field{}, err
	}
	return field.Field{
		Path:  pbField.Path,
		Value: value,
	}, nil
}

// ToFieldValue converts a field value represented in protobuf to a field value in internal
// representation.
func ToFieldValue(pbValue servicepb.FieldValue) (field.ValueUnion, error) {
	var fv field.ValueUnion
	switch pbValue.Type {
	case servicepb.FieldValue_NULL:
		fv.Type = field.NullType
	case servicepb.FieldValue_BOOL:
		fv.Type = field.BoolType
		fv.BoolVal = pbValue.BoolVal
	case servicepb.FieldValue_INT:
		fv.Type = field.IntType
		fv.IntVal = int(pbValue.IntVal)
	case servicepb.FieldValue_DOUBLE:
		fv.Type = field.DoubleType
		fv.DoubleVal = pbValue.DoubleVal
	case servicepb.FieldValue_STRING:
		fv.Type = field.StringType
		fv.StringVal = safe.ToString(pbValue.StringVal)
	case servicepb.FieldValue_TIME:
		fv.Type = field.TimeType
		fv.TimeNanosVal = pbValue.TimeNanosVal
	default:
		return fv, fmt.Errorf("unknown proto field type: %v", pbValue.Type)
	}
	return fv, nil
}
