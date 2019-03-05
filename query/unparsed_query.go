package query

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	"github.com/xichen2020/eventdb/x/convert"
	"github.com/xichen2020/eventdb/x/hash"
	protoconvert "github.com/xichen2020/eventdb/x/proto/convert"
	xtime "github.com/xichen2020/eventdb/x/time"

	m3xtime "github.com/m3db/m3x/time"
)

const (
	defaultTimeUnit                = xtime.Unit(m3xtime.Second)
	defaultFilterCombinator        = filter.And
	defaultRawQuerySizeLimit       = 100
	defaultGroupedQuerySizeLimit   = 10
	defaultOrderBySortOrder        = Ascending
	maxGranularityRangeScaleFactor = 10
	minGranularityRangeScaleFactor = 1000
)

var (
	errNoNamespaceInQuery                  = errors.New("no namespace provided in query")
	errStartAndEndRequiredWithNoRange      = errors.New("both start time and end time required when no time range is specified")
	errNoFieldInFilter                     = errors.New("no field specified in query filter")
	errNoFieldInOrderByForRawQuery         = errors.New("no field specified for raw query")
	errNoFieldOrOpInOrderByForGroupedQuery = errors.New("no field or op specified in order by for grouped query")
	errTimeGranularityWithGroupBy          = errors.New("both time granularity and group by clauses are specified in the query")
	errTimeGranularityWithCalculations     = errors.New("both time granularity and calculation clauses are specified in the query")
	errTimeGranularityWithOrderBy          = errors.New("both time granularity and order by clauses are specified in the query")
	errCalculationsWithNoGroups            = errors.New("calculations provided with no groups")
	errNoTimeGranularityInTimeBucketQuery  = errors.New("no time granularity in time bucket query")
)

// TODO(xichen): Use different types for raw/grouped/bucket queries.
// Define different type aliases for now.

// UnparsedRawQuery is an unparsed raw query.
type UnparsedRawQuery UnparsedQuery

// Parse parses an unparsed raw query into a parsed raw query.
func (q *UnparsedRawQuery) Parse(opts ParseOptions) (ParsedRawQuery, error) {
	parsed, err := ((*UnparsedQuery)(q)).Parse(opts)
	if err != nil {
		return ParsedRawQuery{}, err
	}
	return parsed.RawQuery()
}

// ToProto converts an unparsed raw query to an unparsed raw query protobuf message.
func (q *UnparsedRawQuery) ToProto() (*servicepb.RawQuery, error) {
	tu, err := q.TimeUnit.ToProto()
	if err != nil {
		return nil, err
	}
	filterLists, err := q.Filters.ToProto()
	if err != nil {
		return nil, err
	}
	orderByList, err := q.OrderBy.ToProto()
	if err != nil {
		return nil, err
	}
	return &servicepb.RawQuery{
		Namespace:        q.Namespace,
		StartTime:        protoconvert.Int64PtrToOptionalInt64(q.StartTime),
		EndTime:          protoconvert.Int64PtrToOptionalInt64(q.EndTime),
		TimeUnit:         tu,
		TimeRangeInNanos: protoconvert.DurationPtrToOptionalInt64(q.TimeRange),
		Filters:          filterLists,
		OrderBy:          orderByList,
		Limit:            protoconvert.IntPtrToOptionalInt64(q.Limit),
	}, nil
}

// UnparsedGroupedQuery is an unparsed grouped query.
type UnparsedGroupedQuery UnparsedQuery

// Parse parses an unparsed grouped query into a parsed grouped query.
func (q *UnparsedGroupedQuery) Parse(opts ParseOptions) (ParsedGroupedQuery, error) {
	parsed, err := ((*UnparsedQuery)(q)).Parse(opts)
	if err != nil {
		return ParsedGroupedQuery{}, err
	}
	return parsed.GroupedQuery()
}

// ToProto converts an unparsed grouped query to an unparsed grouped query protobuf message.
func (q *UnparsedGroupedQuery) ToProto() (*servicepb.GroupedQuery, error) {
	tu, err := q.TimeUnit.ToProto()
	if err != nil {
		return nil, err
	}
	filterLists, err := q.Filters.ToProto()
	if err != nil {
		return nil, err
	}
	calculations, err := q.Calculations.ToProto()
	if err != nil {
		return nil, err
	}
	orderByList, err := q.OrderBy.ToProto()
	if err != nil {
		return nil, err
	}
	return &servicepb.GroupedQuery{
		Namespace:        q.Namespace,
		StartTime:        protoconvert.Int64PtrToOptionalInt64(q.StartTime),
		EndTime:          protoconvert.Int64PtrToOptionalInt64(q.EndTime),
		TimeUnit:         tu,
		TimeRangeInNanos: protoconvert.DurationPtrToOptionalInt64(q.TimeRange),
		Filters:          filterLists,
		GroupBy:          q.GroupBy,
		Calculations:     calculations,
		OrderBy:          orderByList,
		Limit:            protoconvert.IntPtrToOptionalInt64(q.Limit),
	}, nil
}

// UnparsedTimeBucketQuery is an unparsed time bucket query.
type UnparsedTimeBucketQuery UnparsedQuery

// Parse parses an unparsed grouped query into a parsed grouped query.
func (q *UnparsedTimeBucketQuery) Parse(opts ParseOptions) (ParsedTimeBucketQuery, error) {
	parsed, err := ((*UnparsedQuery)(q)).Parse(opts)
	if err != nil {
		return ParsedTimeBucketQuery{}, err
	}
	return parsed.TimeBucketQuery()
}

// ToProto converts an unparsed time bucket query to an unparsed time bucket query protobuf message.
func (q *UnparsedTimeBucketQuery) ToProto() (*servicepb.TimeBucketQuery, error) {
	if q.TimeGranularity == nil {
		return nil, errNoTimeGranularityInTimeBucketQuery
	}
	tu, err := q.TimeUnit.ToProto()
	if err != nil {
		return nil, err
	}
	filterLists, err := q.Filters.ToProto()
	if err != nil {
		return nil, err
	}
	return &servicepb.TimeBucketQuery{
		Namespace:              q.Namespace,
		StartTime:              protoconvert.Int64PtrToOptionalInt64(q.StartTime),
		EndTime:                protoconvert.Int64PtrToOptionalInt64(q.EndTime),
		TimeUnit:               tu,
		TimeRangeInNanos:       protoconvert.DurationPtrToOptionalInt64(q.TimeRange),
		TimeGranularityInNanos: (time.Duration)(*q.TimeGranularity).Nanoseconds(),
		Filters:                filterLists,
	}, nil
}

// UnparsedQuery represents an unparsed document query useful for serializing/deserializing in JSON.
type UnparsedQuery struct {
	// Namespace the query will be executed against.
	// NB: This does not allow cross-namespace searches, but fine for now.
	Namespace string `json:"namespace"`

	// Time range portion of the query.
	StartTime       *int64          `json:"start_time"`
	EndTime         *int64          `json:"end_time"`
	TimeUnit        *xtime.Unit     `json:"time_unit"`
	TimeRange       *xtime.Duration `json:"time_range"`
	TimeGranularity *xtime.Duration `json:"time_granularity"`

	// Filters.
	Filters RawFilterLists `json:"filters"`

	// A list of fields to group the results by.
	GroupBy []string `json:"group_by"`

	// A list of calculations to perform within each group.
	// If no groups are specified, the calculations are performed against
	// the entire group.
	Calculations RawCalculations `json:"calculations"`

	// A list of criteria to order the results by. Each criteria must appear
	// either in the group by list or in the calculations list.
	OrderBy RawOrderBys `json:"order_by"`

	// Maximum number of results returned.
	Limit *int `json:"limit"`
}

// RawCalculations is a list of raw calculations.
type RawCalculations []RawCalculation

// ToProto converts a list of raw calculation clauses to a list of calculation protobuf messages.
func (calcs RawCalculations) ToProto() ([]servicepb.Calculation, error) {
	if len(calcs) == 0 {
		return nil, nil
	}
	res := make([]servicepb.Calculation, 0, len(calcs))
	for _, calc := range calcs {
		pbCalculation, err := calc.ToProto()
		if err != nil {
			return nil, err
		}
		res = append(res, pbCalculation)
	}
	return res, nil
}

// RawCalculation represents a raw calculation object.
type RawCalculation struct {
	Field *string        `json:"field"`
	Op    calculation.Op `json:"op"`
}

// ToProto converts a raw calculation to a calculation protobuf message.
func (calc RawCalculation) ToProto() (servicepb.Calculation, error) {
	pbOp, err := calc.Op.ToProto()
	if err != nil {
		return servicepb.Calculation{}, err
	}
	return servicepb.Calculation{
		Field: protoconvert.StringPtrToOptionalString(calc.Field),
		Op:    pbOp,
	}, nil
}

// RawOrderBys is a list of raw order by clauses.
type RawOrderBys []RawOrderBy

// ToProto converts a list of raw order by clauses to a list of order by protobuf messages.
func (obs RawOrderBys) ToProto() ([]servicepb.OrderBy, error) {
	if len(obs) == 0 {
		return nil, nil
	}
	res := make([]servicepb.OrderBy, 0, len(obs))
	for _, ob := range obs {
		pbOrderBy, err := ob.ToProto()
		if err != nil {
			return nil, err
		}
		res = append(res, pbOrderBy)
	}
	return res, nil
}

// RawOrderBy represents a list of criteria for ordering results.
type RawOrderBy struct {
	Field *string         `json:"field"`
	Op    *calculation.Op `json:"op"`
	Order *SortOrder      `json:"order"`
}

// ToProto converts an order by clause to an order by protobuf message.
func (ob RawOrderBy) ToProto() (servicepb.OrderBy, error) {
	pbOp, err := ob.Op.ToOptionalProto()
	if err != nil {
		return servicepb.OrderBy{}, err
	}
	pbOrder, err := ob.Order.ToProto()
	if err != nil {
		return servicepb.OrderBy{}, err
	}
	return servicepb.OrderBy{
		Field: protoconvert.StringPtrToOptionalString(ob.Field),
		Op:    pbOp,
		Order: pbOrder,
	}, nil
}

// ParseOptions provide a set of options for parsing a raw query.
type ParseOptions struct {
	FieldPathSeparator    byte
	FieldHashFn           hash.StringArrayHashFn
	TimestampFieldPath    []string
	RawDocSourceFieldPath []string
}

// Parse parses the raw query, returning any errors encountered.
func (q *UnparsedQuery) Parse(opts ParseOptions) (ParsedQuery, error) {
	sq := ParsedQuery{opts: opts}

	ns, err := q.parseNamespace()
	if err != nil {
		return sq, err
	}
	sq.Namespace = ns

	startNanos, endNanos, granularity, err := q.parseTime()
	if err != nil {
		return sq, err
	}
	sq.StartTimeNanos = startNanos
	sq.EndTimeNanos = endNanos
	sq.TimeGranularity = granularity

	filters, err := q.parseFilters(opts)
	if err != nil {
		return sq, err
	}
	sq.Filters = filters

	groupBy, err := q.parseGroupBy(opts)
	if err != nil {
		return sq, err
	}
	sq.GroupBy = groupBy

	calculations, err := q.parseCalculations(opts)
	if err != nil {
		return sq, err
	}
	sq.Calculations = calculations

	orderBy, err := q.parseOrderByList(sq.GroupBy, sq.Calculations, opts)
	if err != nil {
		return sq, err
	}
	sq.OrderBy = orderBy

	limit, err := q.parseLimit()
	if err != nil {
		return sq, err
	}
	sq.Limit = limit

	err = sq.computeDerived()
	return sq, err
}

func (q *UnparsedQuery) parseNamespace() (string, error) {
	if err := q.validateNamespace(); err != nil {
		return "", err
	}
	return q.Namespace, nil
}

func (q *UnparsedQuery) validateNamespace() error {
	if len(q.Namespace) == 0 {
		return errNoNamespaceInQuery
	}
	return nil
}

func (q *UnparsedQuery) parseTime() (
	startNanos, endNanos int64,
	granularity *time.Duration,
	err error,
) {
	if err := q.validateTime(); err != nil {
		return 0, 0, nil, err
	}

	timeUnit := defaultTimeUnit
	if q.TimeUnit != nil {
		timeUnit = *q.TimeUnit
	}
	unitDurationNanos := int64(timeUnit.MustValue())

	if q.TimeRange == nil {
		startNanos = *q.StartTime * unitDurationNanos
		endNanos = *q.EndTime * unitDurationNanos
	} else if q.StartTime != nil {
		startNanos = *q.StartTime * unitDurationNanos
		endNanos = startNanos + time.Duration(*q.TimeRange).Nanoseconds()
	} else {
		if q.EndTime != nil {
			endNanos = *q.EndTime * unitDurationNanos
		} else {
			endNanos = time.Now().UnixNano()
		}
		startNanos = endNanos - time.Duration(*q.TimeRange).Nanoseconds()
	}

	if q.TimeGranularity == nil {
		return startNanos, endNanos, nil, nil
	}

	// Further validation on query granularity.
	var (
		timeGranularity       = time.Duration(*q.TimeGranularity)
		rangeInNanos          = endNanos - startNanos
		maxGranularityAllowed = rangeInNanos / maxGranularityRangeScaleFactor
		minGranularityAllowed = rangeInNanos / minGranularityRangeScaleFactor
	)
	if timeGranularity.Nanoseconds() > maxGranularityAllowed {
		return 0, 0, nil, fmt.Errorf("query granularity %v is above maximum allowed %v", timeGranularity, time.Duration(maxGranularityAllowed))
	}
	if timeGranularity.Nanoseconds() < minGranularityAllowed {
		return 0, 0, nil, fmt.Errorf("query granularity %v is below minimum allowed %v", timeGranularity, time.Duration(minGranularityAllowed))
	}
	return startNanos, endNanos, &timeGranularity, nil
}

func (q *UnparsedQuery) validateTime() error {
	if q.StartTime != nil && *q.StartTime < 0 {
		return fmt.Errorf("invalid query start time: %d", *q.StartTime)
	}
	if q.EndTime != nil && *q.EndTime < 0 {
		return fmt.Errorf("invalid query end time: %d", *q.EndTime)
	}
	if q.TimeRange == nil && (q.StartTime == nil || q.EndTime == nil) {
		return errStartAndEndRequiredWithNoRange
	}
	if q.TimeRange != nil && *q.TimeRange <= 0 {
		return fmt.Errorf("invalid query time range: %v", q.TimeRange.String())
	}
	if q.TimeRange != nil && q.StartTime != nil && q.EndTime != nil {
		return fmt.Errorf("query start time %d, end time %d, and time range %v are specified but only two should be included", *q.StartTime, *q.EndTime, *q.TimeRange)
	}
	if q.TimeGranularity == nil {
		return nil
	}
	if *q.TimeGranularity <= 0 {
		return fmt.Errorf("invalid query time granularity: %v", q.TimeGranularity.String())
	}
	return nil
}

func (q *UnparsedQuery) parseFilters(opts ParseOptions) ([]FilterList, error) {
	if err := q.validateFilters(); err != nil {
		return nil, err
	}
	flArray := make([]FilterList, 0, len(q.Filters))
	for _, rfl := range q.Filters {
		fts := make([]Filter, 0, len(rfl.Filters))
		for _, rf := range rfl.Filters {
			var f Filter
			f.FieldPath = parseField(rf.Field, opts.FieldPathSeparator)
			f.Op = rf.Op
			if rf.Value != nil {
				switch value := rf.Value.(type) {
				case bool:
					f.Value = &field.ValueUnion{Type: field.BoolType, BoolVal: value}
				case float64:
					if intval, ok := convert.TryAsInt(value); ok {
						f.Value = &field.ValueUnion{Type: field.IntType, IntVal: intval}
					} else {
						f.Value = &field.ValueUnion{Type: field.DoubleType, DoubleVal: value}
					}
				case string:
					f.Value = &field.ValueUnion{Type: field.StringType, StringVal: value}
				default:
					return nil, fmt.Errorf("unknown value type %T", rf.Value)
				}
			}
			fts = append(fts, f)
		}
		fc := defaultFilterCombinator
		if rfl.FilterCombinator != nil {
			fc = *rfl.FilterCombinator
		}
		fl := FilterList{Filters: fts, FilterCombinator: fc}
		flArray = append(flArray, fl)
	}
	return flArray, nil
}

func (q *UnparsedQuery) validateFilters() error {
	for _, f := range q.Filters {
		if err := q.validateFilterList(f.Filters); err != nil {
			return err
		}
	}
	return nil
}

func (q *UnparsedQuery) validateFilterList(filters []RawFilter) error {
	for _, f := range filters {
		if len(f.Field) == 0 {
			return errNoFieldInFilter
		}
		switch f.Op {
		case filter.Exists, filter.DoesNotExist, filter.IsNull, filter.IsNotNull:
			if f.Value != nil {
				return fmt.Errorf("non-empty filter value %v for op %s", f.Value, f.Op.String())
			}
		default:
			if f.Value == nil {
				return fmt.Errorf("empty filter value for op %s", f.Op.String())
			}
		}
	}
	return nil
}

func (q *UnparsedQuery) parseGroupBy(opts ParseOptions) ([][]string, error) {
	if len(q.GroupBy) == 0 {
		return nil, nil
	}
	if err := q.validateGroupBy(); err != nil {
		return nil, err
	}
	groupByFieldPaths := make([][]string, 0, len(q.GroupBy))
	for _, gb := range q.GroupBy {
		fieldPaths := parseField(gb, opts.FieldPathSeparator)
		groupByFieldPaths = append(groupByFieldPaths, fieldPaths)
	}
	return groupByFieldPaths, nil
}

func (q *UnparsedQuery) validateGroupBy() error {
	if q.TimeGranularity != nil {
		return errTimeGranularityWithGroupBy
	}
	return nil
}

func (q *UnparsedQuery) parseCalculations(opts ParseOptions) ([]Calculation, error) {
	if len(q.Calculations) == 0 {
		return nil, nil
	}
	if err := q.validateCalculations(); err != nil {
		return nil, err
	}
	calculations := make([]Calculation, 0, len(q.Calculations))
	for _, rawCalc := range q.Calculations {
		var calc Calculation
		if rawCalc.Field != nil {
			calc.FieldPath = parseField(*rawCalc.Field, opts.FieldPathSeparator)
		}
		calc.Op = rawCalc.Op
		calculations = append(calculations, calc)
	}
	return calculations, nil
}

func (q *UnparsedQuery) validateCalculations() error {
	if q.TimeGranularity != nil {
		return errTimeGranularityWithCalculations
	}
	if len(q.GroupBy) == 0 {
		return errCalculationsWithNoGroups
	}
	// If the calculation operation is count, the field should be omitted,
	// in which case the number of events containing the corresponding group key
	// is returned. All other operations require the field be specified.
	for _, calc := range q.Calculations {
		if calc.Field != nil && !calc.Op.RequiresField() {
			return fmt.Errorf("field %v cannot be specified for calculation op %v that does not require a field", *calc.Field, calc.Op)
		}
		if calc.Field == nil && calc.Op.RequiresField() {
			return fmt.Errorf("no field specified for calculation op %v", calc.Op)
		}
	}
	return nil
}

func (q *UnparsedQuery) parseOrderByList(
	groupBy [][]string,
	calculations []Calculation,
	opts ParseOptions,
) ([]OrderBy, error) {
	if len(q.OrderBy) == 0 {
		return nil, nil
	}
	if q.TimeGranularity != nil {
		return nil, errTimeGranularityWithOrderBy
	}
	obArray := make([]OrderBy, 0, len(q.OrderBy))
	for _, rob := range q.OrderBy {
		ob, err := q.parseOrderBy(rob, groupBy, calculations, opts)
		if err != nil {
			return nil, err
		}
		obArray = append(obArray, ob)
	}
	return obArray, nil
}

func (q *UnparsedQuery) parseOrderBy(
	rob RawOrderBy,
	groupBy [][]string,
	calculations []Calculation,
	opts ParseOptions,
) (OrderBy, error) {
	var ob OrderBy
	ob.SortOrder = defaultOrderBySortOrder
	if rob.Order != nil {
		ob.SortOrder = *rob.Order
	}

	if len(q.GroupBy) == 0 {
		// This is a raw document query.
		if rob.Field == nil {
			return ob, errNoFieldInOrderByForRawQuery
		}
		ob.FieldType = RawField
		ob.FieldPath = parseField(*rob.Field, opts.FieldPathSeparator)
		return ob, nil
	}

	// Otherwise this is a grouped query.
	if rob.Field == nil || rob.Op != nil {
		if rob.Field == nil && rob.Op == nil {
			return ob, errNoFieldOrOpInOrderByForGroupedQuery
		}
		if rob.Field != nil && !rob.Op.RequiresField() {
			return ob, fmt.Errorf("field specified %s for order by op %v", *rob.Field, *rob.Op)
		}
		if rob.Field == nil && rob.Op.RequiresField() {
			return ob, fmt.Errorf("no field specified for order by op %v", *rob.Op)
		}
		for idx, calc := range q.Calculations {
			if calc.Field == nil && rob.Field != nil {
				continue
			}
			if calc.Field != nil && rob.Field == nil {
				continue
			}
			if calc.Field != nil && rob.Field != nil && *calc.Field != *rob.Field {
				continue
			}
			if calc.Op == *rob.Op {
				ob.FieldType = CalculationField
				ob.FieldIndex = idx
				ob.FieldPath = calculations[idx].FieldPath
				return ob, nil
			}
		}
		return ob, fmt.Errorf("invalid order by clause: %v", rob)
	}

	// Now try to find the field in the group by list.
	for idx, f := range q.GroupBy {
		if *rob.Field == f {
			ob.FieldType = GroupByField
			ob.FieldIndex = idx
			ob.FieldPath = groupBy[idx]
			return ob, nil
		}
	}

	return ob, fmt.Errorf("invalid order by clause: %v", rob)
}

// TODO(xichen): Protect against overly aggressive limits.
func (q *UnparsedQuery) parseLimit() (int, error) {
	if err := q.validateLimit(); err != nil {
		return 0, err
	}
	if q.Limit != nil && *q.Limit >= 0 {
		return *q.Limit, nil
	}
	if len(q.GroupBy) == 0 {
		return defaultRawQuerySizeLimit, nil
	}
	return defaultGroupedQuerySizeLimit, nil
}

func (q UnparsedQuery) validateLimit() error {
	if q.Limit != nil && *q.Limit <= 0 {
		return fmt.Errorf("invalid query result limit: %d", *q.Limit)
	}
	return nil
}

// parseField parses a concatenated field name (e.g., foo.bar.baz)
// into an array of field path segments (e.g., []string{"foo", "bar", "baz"})
// given the field path separator.
func parseField(fieldName string, separator byte) []string {
	return strings.Split(fieldName, string(separator))
}
