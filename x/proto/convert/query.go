package convert

import (
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/query"
	xtime "github.com/xichen2020/eventdb/x/time"

	m3xtime "github.com/m3db/m3x/time"
)

// ToUnparsedRawQuery converts a raw query represented in protobuf to
// an unparsed raw query.
func ToUnparsedRawQuery(
	q *servicepb.RawQuery,
) (query.UnparsedQuery, error) {
	tu, err := ToTimeUnitPtr(q.TimeUnit)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	filterLists, err := ToFilterLists(q.Filters)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	orderByList, err := ToOrderByList(q.OrderBy)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	return query.UnparsedQuery{
		Namespace: q.Namespace,
		StartTime: ToInt64Ptr(q.StartTime),
		EndTime:   ToInt64Ptr(q.EndTime),
		TimeUnit:  tu,
		TimeRange: ToDurationPtr(q.TimeRangeInNanos),
		Filters:   filterLists,
		OrderBy:   orderByList,
		Limit:     ToIntPtr(q.Limit),
	}, nil
}

// ToUnparsedGroupedQuery converts a grouped query represented in protobuf to
// an unparsed grouped query.
func ToUnparsedGroupedQuery(
	q *servicepb.GroupedQuery,
) (query.UnparsedQuery, error) {
	tu, err := ToTimeUnitPtr(q.TimeUnit)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	filterLists, err := ToFilterLists(q.Filters)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	calculations, err := ToCalculations(q.Calculations)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	orderByList, err := ToOrderByList(q.OrderBy)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	return query.UnparsedQuery{
		Namespace:    q.Namespace,
		StartTime:    ToInt64Ptr(q.StartTime),
		EndTime:      ToInt64Ptr(q.EndTime),
		TimeUnit:     tu,
		TimeRange:    ToDurationPtr(q.TimeRangeInNanos),
		Filters:      filterLists,
		GroupBy:      q.GroupBy,
		Calculations: calculations,
		OrderBy:      orderByList,
		Limit:        ToIntPtr(q.Limit),
	}, nil
}

// ToUnparsedTimeBucketQuery converts a time bucket query represented in protobuf to
// an unparsed time bucket query.
func ToUnparsedTimeBucketQuery(
	q *servicepb.TimeBucketQuery,
) (query.UnparsedQuery, error) {
	tu, err := ToTimeUnitPtr(q.TimeUnit)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	filterLists, err := ToFilterLists(q.Filters)
	if err != nil {
		return query.UnparsedQuery{}, err
	}

	granularity := xtime.Duration(q.TimeGranularityInNanos)
	return query.UnparsedQuery{
		Namespace:       q.Namespace,
		StartTime:       ToInt64Ptr(q.StartTime),
		EndTime:         ToInt64Ptr(q.EndTime),
		TimeUnit:        tu,
		TimeRange:       ToDurationPtr(q.TimeRangeInNanos),
		TimeGranularity: &granularity,
		Filters:         filterLists,
	}, nil
}

// ToIntPtr converts an optional int64 in protobuf to an *int.
func ToIntPtr(pbValue servicepb.OptionalInt64) *int {
	if pbValue.GetNoValue() {
		return nil
	}
	v := int(pbValue.GetData())
	return &v
}

// ToInt64Ptr converts an optional int64 in protobuf to an *int64.
func ToInt64Ptr(pbValue servicepb.OptionalInt64) *int64 {
	if pbValue.GetNoValue() {
		return nil
	}
	v := pbValue.GetData()
	return &v
}

// ToDurationPtr converts an optional int64 in protobuf to a time duration pointer.
func ToDurationPtr(pbTimeNanos servicepb.OptionalInt64) *xtime.Duration {
	if pbTimeNanos.GetNoValue() {
		return nil
	}
	dur := xtime.Duration(pbTimeNanos.GetData())
	return &dur
}

// ToTimeUnitPtr converts an optional time unit in protobuf to an internal time unit pointer.
func ToTimeUnitPtr(pbTimeUnit servicepb.OptionalTimeUnit) (*query.TimeUnit, error) {
	if pbTimeUnit.GetNoValue() {
		return nil, nil
	}
	tu, err := ToTimeUnit(pbTimeUnit.GetData())
	if err != nil {
		return nil, err
	}
	return &tu, nil
}

// ToTimeUnit converts a time unit in protobuf to an internal time unit.
func ToTimeUnit(pbTimeUnit servicepb.TimeUnit) (query.TimeUnit, error) {
	switch pbTimeUnit {
	case servicepb.TimeUnit_SECOND:
		return query.TimeUnit(m3xtime.Second), nil
	case servicepb.TimeUnit_MILLISECOND:
		return query.TimeUnit(m3xtime.Millisecond), nil
	case servicepb.TimeUnit_MICROSECOND:
		return query.TimeUnit(m3xtime.Microsecond), nil
	case servicepb.TimeUnit_NANOSECOND:
		return query.TimeUnit(m3xtime.Nanosecond), nil
	case servicepb.TimeUnit_MINUTE:
		return query.TimeUnit(m3xtime.Minute), nil
	case servicepb.TimeUnit_HOUR:
		return query.TimeUnit(m3xtime.Hour), nil
	case servicepb.TimeUnit_DAY:
		return query.TimeUnit(m3xtime.Day), nil
	case servicepb.TimeUnit_YEAR:
		return query.TimeUnit(m3xtime.Year), nil
	default:
		return 0, fmt.Errorf("invalid protobuf time unit %v", pbTimeUnit)
	}
}

// ToFilterLists converts filter lists in protobuf to a list of internal filter lists.
func ToFilterLists(pbFilterLists []servicepb.FilterList) ([]query.RawFilterList, error) {
	res := make([]query.RawFilterList, 0, len(pbFilterLists))
	for _, pbFilterList := range pbFilterLists {
		filterList, err := ToFilterList(pbFilterList)
		if err != nil {
			return nil, err
		}
		res = append(res, filterList)
	}
	return res, nil
}

// ToFilterList converts a filter list in protobuf to an internal filter list.
func ToFilterList(pbFilterList servicepb.FilterList) (query.RawFilterList, error) {
	filters, err := ToFilters(pbFilterList.Filters)
	if err != nil {
		return query.RawFilterList{}, err
	}
	filterCombinator, err := ToFilterCombinatorPtr(pbFilterList.FilterCombinator)
	if err != nil {
		return query.RawFilterList{}, err
	}
	return query.RawFilterList{
		Filters:          filters,
		FilterCombinator: filterCombinator,
	}, nil
}

// ToFilters converts a list of filters in protobuf to an internal list of filters.
func ToFilters(pbFilters []servicepb.Filter) ([]query.RawFilter, error) {
	filters := make([]query.RawFilter, 0, len(pbFilters))
	for _, pbFilter := range pbFilters {
		filter, err := ToFilter(pbFilter)
		if err != nil {
			return nil, err
		}
		filters = append(filters, filter)
	}
	return filters, nil
}

// ToFilter converts a filter in protobuf to an internal filter.
func ToFilter(pbFilter servicepb.Filter) (query.RawFilter, error) {
	op, err := ToFilterOp(pbFilter.Op)
	if err != nil {
		return query.RawFilter{}, err
	}
	value, err := ToFilterValue(pbFilter.Value)
	if err != nil {
		return query.RawFilter{}, err
	}
	return query.RawFilter{
		Field: pbFilter.Field,
		Op:    op,
		Value: value,
	}, nil
}

// ToFilterOp converts a filter op in protobuf to an internal filter op.
func ToFilterOp(pbFilterOp servicepb.Filter_Op) (filter.Op, error) {
	switch pbFilterOp {
	case servicepb.Filter_EQUALS:
		return filter.Equals, nil
	case servicepb.Filter_NOTEQUALS:
		return filter.NotEquals, nil
	case servicepb.Filter_LARGERTHAN:
		return filter.LargerThan, nil
	case servicepb.Filter_LARGERTHANOREQUAL:
		return filter.LargerThanOrEqual, nil
	case servicepb.Filter_SMALLERTHAN:
		return filter.SmallerThan, nil
	case servicepb.Filter_SMALLERTHANOREQUAL:
		return filter.SmallerThanOrEqual, nil
	case servicepb.Filter_STARTSWITH:
		return filter.StartsWith, nil
	case servicepb.Filter_DOESNOTSTARTWITH:
		return filter.DoesNotStartWith, nil
	case servicepb.Filter_ENDSWITH:
		return filter.EndsWith, nil
	case servicepb.Filter_DOESNOTENDWITH:
		return filter.DoesNotEndWith, nil
	case servicepb.Filter_CONTAINS:
		return filter.Contains, nil
	case servicepb.Filter_DOESNOTCONTAIN:
		return filter.DoesNotContain, nil
	case servicepb.Filter_ISNULL:
		return filter.IsNull, nil
	case servicepb.Filter_ISNOTNULL:
		return filter.IsNotNull, nil
	case servicepb.Filter_EXISTS:
		return filter.Exists, nil
	case servicepb.Filter_DOESNOTEXIST:
		return filter.DoesNotExist, nil
	default:
		return filter.UnknownOp, fmt.Errorf("invalid protobuf filter op %v", pbFilterOp)
	}
}

// ToFilterValue converts an optional filter value to an internal filter value.
func ToFilterValue(pbFilterValue servicepb.OptionalFilterValue) (interface{}, error) {
	if pbFilterValue.GetNoValue() {
		return nil, nil
	}
	fv := pbFilterValue.GetData()
	switch fv.Type {
	case servicepb.FilterValue_BOOL:
		return fv.BoolVal, nil
	case servicepb.FilterValue_NUMBER:
		return fv.NumberVal, nil
	case servicepb.FilterValue_STRING:
		return fv.StringVal, nil
	default:
		return nil, fmt.Errorf("invalid protobuf filter value type %v", fv.Type)
	}
}

// ToFilterCombinatorPtr converts an optional filter combinator in protobuf
// to a filter combinator pointer.
func ToFilterCombinatorPtr(
	pbFilterCombinator servicepb.OptionalFilterCombinator,
) (*filter.Combinator, error) {
	if pbFilterCombinator.GetNoValue() {
		return nil, nil
	}
	var combinator filter.Combinator
	pbCombinator := pbFilterCombinator.GetData()
	switch pbCombinator {
	case servicepb.FilterCombinator_AND:
		combinator = filter.And
	case servicepb.FilterCombinator_OR:
		combinator = filter.Or
	default:
		return nil, fmt.Errorf("invalid protobuf filter combinator %v", pbCombinator)
	}
	return &combinator, nil
}

// ToOrderByList converts a list of orderBy clauses in protobuf to a list of
// internal orderBy objects.
func ToOrderByList(pbOrderByList []servicepb.OrderBy) ([]query.RawOrderBy, error) {
	orderBys := make([]query.RawOrderBy, 0, len(pbOrderByList))
	for _, pbOrderBy := range pbOrderByList {
		orderBy, err := ToOrderBy(pbOrderBy)
		if err != nil {
			return nil, err
		}
		orderBys = append(orderBys, orderBy)
	}
	return orderBys, nil
}

// ToOrderBy converts an orderBy clauses in protobuf to an internal orderBy object.
func ToOrderBy(pbOrderBy servicepb.OrderBy) (query.RawOrderBy, error) {
	op, err := ToCalculationOpPtr(pbOrderBy.Op)
	if err != nil {
		return query.RawOrderBy{}, err
	}
	order, err := ToSortOrderPtr(pbOrderBy.Order)
	if err != nil {
		return query.RawOrderBy{}, err
	}
	return query.RawOrderBy{
		Field: ToStringPtr(pbOrderBy.Field),
		Op:    op,
		Order: order,
	}, nil
}

// ToStringPtr converts an optional string in protobuf to a string pointer.
func ToStringPtr(pbString servicepb.OptionalString) *string {
	if pbString.GetNoValue() {
		return nil
	}
	v := pbString.GetData()
	return &v
}

// ToCalculations converts a list of calculation clauses in protobuf to a list of
// internal calculation objects.
func ToCalculations(pbCalculations []servicepb.Calculation) ([]query.RawCalculation, error) {
	res := make([]query.RawCalculation, 0, len(pbCalculations))
	for _, pbCalculation := range pbCalculations {
		calc, err := ToCalculation(pbCalculation)
		if err != nil {
			return nil, err
		}
		res = append(res, calc)
	}
	return res, nil
}

// ToCalculation converts a calculation in protobuf to an internal calculation object.
func ToCalculation(pbCalculation servicepb.Calculation) (query.RawCalculation, error) {
	op, err := ToCalculationOp(pbCalculation.Op)
	if err != nil {
		return query.RawCalculation{}, err
	}
	return query.RawCalculation{
		Field: ToStringPtr(pbCalculation.Field),
		Op:    op,
	}, nil
}

// ToCalculationOpPtr converts an optional calculation op in protobuf to an internal
// calculation op pointer.
func ToCalculationOpPtr(
	pbCalculationOp servicepb.OptionalCalculationOp,
) (*calculation.Op, error) {
	if pbCalculationOp.GetNoValue() {
		return nil, nil
	}
	v, err := ToCalculationOp(pbCalculationOp.GetData())
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// ToCalculationOp converts a calculation op in protobuf to an internal calculation op.
func ToCalculationOp(
	pbCalculationOp servicepb.Calculation_Op,
) (calculation.Op, error) {
	switch pbCalculationOp {
	case servicepb.Calculation_COUNT:
		return calculation.Count, nil
	case servicepb.Calculation_SUM:
		return calculation.Sum, nil
	case servicepb.Calculation_AVG:
		return calculation.Avg, nil
	case servicepb.Calculation_MIN:
		return calculation.Min, nil
	case servicepb.Calculation_MAX:
		return calculation.Max, nil
	default:
		return calculation.UnknownOp, fmt.Errorf("invalid protobuf calculation op %v", pbCalculationOp)
	}
}

// ToSortOrderPtr converts an optional sort order in protobuf to an internal
// sort order pointer.
func ToSortOrderPtr(
	pbSortOrder servicepb.OptionalSortOrder,
) (*query.SortOrder, error) {
	if pbSortOrder.GetNoValue() {
		return nil, nil
	}
	v, err := ToSortOrder(pbSortOrder.GetData())
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// ToSortOrder converts a sort order in protobuf to an internal sort order.
func ToSortOrder(pbSortOrder servicepb.SortOrder) (query.SortOrder, error) {
	switch pbSortOrder {
	case servicepb.SortOrder_ASCENDING:
		return query.Ascending, nil
	case servicepb.SortOrder_DESCENDING:
		return query.Descending, nil
	default:
		return query.UnknownSortOrder, fmt.Errorf("invalid protobuf sort order %v", pbSortOrder)
	}
}
