package template

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values/iterator"
	iterimpl "github.com/xichen2020/eventdb/values/iterator/impl"
)

// defaultFilteredBoolValueIterator creates a default bool value iterator.
func defaultFilteredBoolValueIterator(
	values BoolValueCollection,
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

// defaultFilteredIntValueIterator creates a default int value iterator.
func defaultFilteredIntValueIterator(
	values IntValueCollection,
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

// defaultFilteredDoubleValueIterator creates a default double value iterator.
func defaultFilteredDoubleValueIterator(
	values DoubleValueCollection,
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

// defaultFilteredBytesValueIterator creates a default bytes value iterator.
func defaultFilteredBytesValueIterator(
	values BytesValueCollection,
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	flt, err := op.BytesFilter(filterValue)
	if err != nil {
		return nil, fmt.Errorf("invalid bytes filter op %v with filter value %v", op, filterValue)
	}
	valuesIt, err := values.Iter()
	if err != nil {
		return nil, err
	}
	return iterimpl.NewFilteredBytesIterator(valuesIt, flt), nil
}

// defaultFilteredTimeValueIterator creates a default time value iterator.
func defaultFilteredTimeValueIterator(
	values TimeValueCollection,
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
