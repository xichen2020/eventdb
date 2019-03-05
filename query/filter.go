package query

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
)

// RawFilterLists is a list of raw filter list.
type RawFilterLists []RawFilterList

// ToProto converts a list of raw filter lists to a list of filter lists protobuf messages.
func (rl RawFilterLists) ToProto() ([]servicepb.FilterList, error) {
	if len(rl) == 0 {
		return nil, nil
	}
	res := make([]servicepb.FilterList, 0, len(rl))
	for _, l := range rl {
		pbFilterList, err := l.ToProto()
		if err != nil {
			return nil, err
		}
		res = append(res, pbFilterList)
	}
	return res, nil
}

// RawFilterList is a list of raw filters.
type RawFilterList struct {
	Filters          RawFilters         `json:"filters"`
	FilterCombinator *filter.Combinator `json:"filter_combinator"`
}

// ToProto converts the raw filter list to a filter list protobuf message.
func (l *RawFilterList) ToProto() (servicepb.FilterList, error) {
	filters, err := l.Filters.ToProto()
	if err != nil {
		return servicepb.FilterList{}, err
	}
	filterCombinator, err := l.FilterCombinator.ToProto()
	if err != nil {
		return servicepb.FilterList{}, err
	}
	return servicepb.FilterList{
		Filters:          filters,
		FilterCombinator: filterCombinator,
	}, nil
}

// RawFilters is a list of raw filters
type RawFilters []RawFilter

// ToProto converts a list of raw filters to a list of raw filter protobuf messages.
func (rf RawFilters) ToProto() ([]servicepb.Filter, error) {
	if len(rf) == 0 {
		return nil, nil
	}
	res := make([]servicepb.Filter, 0, len(rf))
	for _, f := range rf {
		pbFilter, err := f.ToProto()
		if err != nil {
			return nil, err
		}
		res = append(res, pbFilter)
	}
	return res, nil
}

// RawFilter represents a raw query filter.
type RawFilter struct {
	Field string      `json:"field"`
	Op    filter.Op   `json:"op"`
	Value interface{} `json:"value"`
}

// ToProto converts the raw filter to a filter protobuf message.
func (l *RawFilter) ToProto() (servicepb.Filter, error) {
	pbOp, err := l.Op.ToProto()
	if err != nil {
		return servicepb.Filter{}, err
	}
	pbFilterValue, err := toOptionalFilterValue(l.Value)
	if err != nil {
		return servicepb.Filter{}, err
	}
	return servicepb.Filter{
		Field: l.Field,
		Op:    pbOp,
		Value: pbFilterValue,
	}, nil
}

// FilterList is a list of parsed filters.
type FilterList struct {
	Filters          []Filter
	FilterCombinator filter.Combinator
}

// Filter is a parsed filter.
// TODO(xichen): Handle range query.
type Filter struct {
	FieldPath []string
	Op        filter.Op
	Value     *field.ValueUnion
}

// AllowedFieldTypes returns a list of allowed field types given the filter
// operator and the RHS value if applicable.
func (f Filter) AllowedFieldTypes() (field.ValueTypeSet, error) {
	return f.Op.AllowedTypes(f.Value)
}

func toOptionalFilterValue(value interface{}) (servicepb.OptionalFilterValue, error) {
	if value == nil {
		noValue := &servicepb.OptionalFilterValue_NoValue{NoValue: true}
		return servicepb.OptionalFilterValue{Value: noValue}, nil
	}
	v := &servicepb.FilterValue{}
	switch value := value.(type) {
	case bool:
		v.Type = servicepb.FilterValue_BOOL
		v.BoolVal = value
	case float64:
		v.Type = servicepb.FilterValue_NUMBER
		v.NumberVal = value
	case string:
		v.Type = servicepb.FilterValue_STRING
		v.StringVal = value
	default:
		return servicepb.OptionalFilterValue{}, fmt.Errorf("invalid filter value %v", value)
	}
	return servicepb.OptionalFilterValue{
		Value: &servicepb.OptionalFilterValue_Data{Data: v},
	}, nil
}
