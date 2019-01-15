// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package decoding

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"

	"github.com/xichen2020/eventdb/filter"

	"github.com/xichen2020/eventdb/values/iterator"

	iterimpl "github.com/xichen2020/eventdb/values/iterator/impl"
)

// defaultFilteredFsBasedBoolValueIterator creates a default bool value iterator.
func defaultFilteredFsBasedBoolValueIterator(
	values *fsBasedBoolValues,
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	flt, err := op.BoolFilter(filterValue)
	if err != nil {
		return nil, fmt.Errorf("invalid bool filter op %v with filter value %v", op, filterValue)
	}
	valuesIt, err := values.Iter()
	if err != nil {
		return nil, err
	}
	return iterimpl.NewFilteredBoolIterator(valuesIt, flt), nil
}

// defaultFilteredFsBasedIntValueIterator creates a default int value iterator.
func defaultFilteredFsBasedIntValueIterator(
	values *fsBasedIntValues,
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	flt, err := op.IntFilter(filterValue)
	if err != nil {
		return nil, fmt.Errorf("invalid int filter op %v with filter value %v", op, filterValue)
	}
	valuesIt, err := values.Iter()
	if err != nil {
		return nil, err
	}
	return iterimpl.NewFilteredIntIterator(valuesIt, flt), nil
}

// defaultFilteredFsBasedDoubleValueIterator creates a default double value iterator.
func defaultFilteredFsBasedDoubleValueIterator(
	values *fsBasedDoubleValues,
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	flt, err := op.DoubleFilter(filterValue)
	if err != nil {
		return nil, fmt.Errorf("invalid double filter op %v with filter value %v", op, filterValue)
	}
	valuesIt, err := values.Iter()
	if err != nil {
		return nil, err
	}
	return iterimpl.NewFilteredDoubleIterator(valuesIt, flt), nil
}

// defaultFilteredFsBasedStringValueIterator creates a default string value iterator.
func defaultFilteredFsBasedStringValueIterator(
	values *fsBasedStringValues,
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	flt, err := op.StringFilter(filterValue)
	if err != nil {
		return nil, fmt.Errorf("invalid string filter op %v with filter value %v", op, filterValue)
	}
	valuesIt, err := values.Iter()
	if err != nil {
		return nil, err
	}
	return iterimpl.NewFilteredStringIterator(valuesIt, flt), nil
}

// defaultFilteredFsBasedTimeValueIterator creates a default time value iterator.
func defaultFilteredFsBasedTimeValueIterator(
	values *fsBasedTimeValues,
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	flt, err := op.TimeFilter(filterValue)
	if err != nil {
		return nil, fmt.Errorf("invalid time filter op %v with filter value %v", op, filterValue)
	}
	valuesIt, err := values.Iter()
	if err != nil {
		return nil, err
	}
	return iterimpl.NewFilteredTimeIterator(valuesIt, flt), nil
}