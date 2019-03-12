package executor

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"

	xerrors "github.com/m3db/m3x/errors"
)

func newSingleTypeFieldIterator(
	fieldPath []string,
	allowedTypes field.ValueTypeSet,
	queryField indexfield.DocsField,
	expectedType *field.ValueType,
) (indexfield.BaseFieldIterator, field.ValueType, error) {
	if len(allowedTypes) != 1 {
		return nil, field.UnknownType, fmt.Errorf("field %s should only have one type but instead have types %v", fieldPath, allowedTypes)
	}
	var t field.ValueType
	for key := range allowedTypes {
		t = key
		break
	}
	if expectedType != nil && t != *expectedType {
		return nil, field.UnknownType, fmt.Errorf("field %s has type %v in the segment but expects type %v", fieldPath, t, *expectedType)
	}
	fu, found := queryField.FieldForType(t)
	if !found {
		return nil, field.UnknownType, fmt.Errorf("field %s does not have values of type %v", fieldPath, t)
	}
	it, err := fu.Iter()
	if err != nil {
		return nil, field.UnknownType, fmt.Errorf("error getting iterator for orderBy field %v type %v", fieldPath, t)
	}
	return it, t, nil
}

func closeIterators(
	multiErr xerrors.MultiError,
	iters []indexfield.BaseFieldIterator,
) xerrors.MultiError {
	for i := range iters {
		if itErr := iters[i].Err(); itErr != nil {
			multiErr = multiErr.Add(itErr)
		}
		iters[i].Close()
		iters[i] = nil
	}
	return multiErr
}
