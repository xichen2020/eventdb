package query

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/x/convert"

	xtime "github.com/m3db/m3x/time"
)

const (
	defaultTimeGranularity    = time.Second
	defaultTimeUnit           = TimeUnit(xtime.Second)
	defaultFilterCombinator   = And
	defaultRawEventQueryLimit = 500
	defaultOrderBySortOrder   = Ascending
)

var (
	errNoNamespaceInQuery                  = errors.New("no namespace provided in query")
	errStartAndEndRequiredWithNoRange      = errors.New("both start time and end time required when no time range is specified")
	errNoFieldInFilter                     = errors.New("no field specified in query filter")
	errNoFieldInOrderByForRawQuery         = errors.New("no field specified for raw query")
	errNoFieldOrOpInOrderByForGroupedQuery = errors.New("no field or op specified in order by for grouped query")
	errCalculationsWithNoGroups            = errors.New("calculations provided with no groups")
)

// RawQuery represents a raw event query useful for serializing/deserializing in JSON.
type RawQuery struct {
	// Namespace the query will be executed against.
	// NB: This does not allow cross-namespace searches, but fine for now.
	Namespace string `json:"namespace"`

	// Time range portion of the query.
	StartTime       *int64         `json:"start_time"`
	EndTime         *int64         `json:"end_time"`
	TimeUnit        *TimeUnit      `json:"time_unit"`
	TimeRange       *time.Duration `json:"time_range"`
	TimeGranularity *time.Duration `json:"time_granularity"`

	// Filters.
	Filters []RawFilterList `json:"filters"`

	// A list of fields to group the results by.
	GroupBy []string `json:"group_by"`

	// A list of calculations to perform within each group.
	// If no groups are specified, the calculations are performed against
	// the entire group.
	Calculations []RawCalculation `json:"calculations"`

	// A list of criteria to order the results by. Each criteria must appear
	// either in the group by list or in the calculations list.
	OrderBy []RawOrderBy `json:"order_by"`

	// Maximum number of results returned.
	Limit *int `json:"limit"`
}

// RawFilterList is a list of raw filters.
type RawFilterList struct {
	Filters          []RawFilter       `json:"filters"`
	FilterCombinator *FilterCombinator `json:"filter_combinator"`
}

// RawFilter represents a raw query filter.
type RawFilter struct {
	Field string      `json:"field"`
	Op    FilterOp    `json:"op"`
	Value interface{} `json:"value"`
}

// RawCalculation represents a raw calculation object.
type RawCalculation struct {
	Field *string       `json:"field"`
	Op    CalculationOp `json:"op"`
}

// RawOrderBy represents a list of criteria for ordering results.
type RawOrderBy struct {
	Field *string        `json:"field"`
	Op    *CalculationOp `json:"op"`
	Order *SortOrder     `json:"order"`
}

// ParseOptions provide a set of options for parsing a raw query.
type ParseOptions struct {
	FieldPathSeparator byte
}

// Parse parses the raw query, returning any errors encountered.
func (q *RawQuery) Parse(opts ParseOptions) (ParsedQuery, error) {
	var sq ParsedQuery

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

	sq.GroupBy = q.GroupBy

	calculations, err := q.parseCalculations(opts)
	if err != nil {
		return sq, err
	}
	sq.Calculations = calculations

	orderBy, err := q.parseOrderByList(opts)
	if err != nil {
		return sq, err
	}
	sq.OrderBy = orderBy

	limit, err := q.parseLimit()
	if err != nil {
		return sq, err
	}
	sq.Limit = limit

	return sq, nil
}

func (q *RawQuery) parseNamespace() (string, error) {
	if err := q.validateNamespace(); err != nil {
		return "", err
	}
	return q.Namespace, nil
}

func (q *RawQuery) validateNamespace() error {
	if len(q.Namespace) == 0 {
		return errNoNamespaceInQuery
	}
	return nil
}

func (q *RawQuery) parseTime() (
	startNanos, endNanos int64,
	granularity time.Duration,
	err error,
) {
	if err := q.validateTime(); err != nil {
		return 0, 0, time.Duration(0), err
	}

	timeUnit := defaultTimeUnit
	if q.TimeUnit != nil {
		timeUnit = *q.TimeUnit
	}
	unitDurationNanos := int64(timeUnit.MustValue())

	granularity = defaultTimeGranularity
	if q.TimeGranularity != nil {
		granularity = *q.TimeGranularity
	}

	if q.TimeRange == nil {
		startNanos = *q.StartTime * unitDurationNanos
		endNanos = *q.EndTime * unitDurationNanos
	} else if q.StartTime != nil {
		startNanos = *q.StartTime * unitDurationNanos
		endNanos = startNanos + q.TimeRange.Nanoseconds()
	} else {
		endNanos = *q.EndTime * unitDurationNanos
		startNanos = endNanos - q.TimeRange.Nanoseconds()
	}
	return startNanos, endNanos, granularity, nil
}

func (q *RawQuery) validateTime() error {
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
	if q.TimeGranularity != nil && *q.TimeGranularity <= 0 {
		return fmt.Errorf("invalid query time granularity: %v", q.TimeGranularity.String())
	}
	return nil
}

func (q *RawQuery) parseFilters(opts ParseOptions) ([]FilterList, error) {
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

func (q *RawQuery) validateFilters() error {
	for _, f := range q.Filters {
		if err := q.validateFilterList(f.Filters); err != nil {
			return err
		}
	}
	return nil
}

func (q *RawQuery) validateFilterList(filters []RawFilter) error {
	for _, f := range filters {
		if len(f.Field) == 0 {
			return errNoFieldInFilter
		}
		switch f.Op {
		case Exists, DoesNotExist, IsNull, IsNotNull:
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

func (q *RawQuery) parseCalculations(opts ParseOptions) ([]Calculation, error) {
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

func (q *RawQuery) validateCalculations() error {
	if len(q.GroupBy) == 0 {
		return errCalculationsWithNoGroups
	}
	for _, calc := range q.Calculations {
		// It's okay to omit the column if the calculation operation is count,
		// in which case the number of events containing the corresponding group key
		// is returned. All other operations require the field be specified.
		if calc.Field == nil && calc.Op != Count {
			return fmt.Errorf("no field specified for calculation op %v", calc.Op)
		}
	}
	return nil
}

func (q *RawQuery) parseOrderByList(opts ParseOptions) ([]OrderBy, error) {
	obArray := make([]OrderBy, 0, len(q.OrderBy))
	for _, rob := range q.OrderBy {
		ob, err := q.parseOrderBy(rob, opts)
		if err != nil {
			return nil, err
		}
		obArray = append(obArray, ob)
	}
	return obArray, nil
}

func (q *RawQuery) parseOrderBy(rob RawOrderBy, opts ParseOptions) (OrderBy, error) {
	var ob OrderBy
	ob.SortOrder = defaultOrderBySortOrder
	if rob.Order != nil {
		ob.SortOrder = *rob.Order
	}

	if len(q.GroupBy) == 0 {
		// This is a raw event query.
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
		if rob.Field == nil && *rob.Op != Count {
			return ob, fmt.Errorf("no field specified for order by op %v", *rob.Op)
		}
		for idx, calc := range q.Calculations {
			if calc.Field == nil && rob.Field != nil {
				continue
			}
			if calc.Field != nil && rob.Field == nil {
				continue
			}
			if calc.Field != nil && rob.Field != nil && calc.Field != rob.Field {
				continue
			}
			if calc.Op == *rob.Op {
				ob.FieldType = CalculationField
				ob.FieldIndex = idx
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
			return ob, nil
		}
	}

	return ob, fmt.Errorf("invalid order by clause: %v", rob)
}

func (q *RawQuery) parseLimit() (*int, error) {
	if err := q.validateLimit(); err != nil {
		return nil, err
	}
	var limit int
	if len(q.GroupBy) == 0 {
		// This is a raw event query, for which the limit is the upper
		// limit on the log event returned. If the limit is nil, a default
		// limit is applied.
		limit = defaultRawEventQueryLimit
		if q.Limit != nil {
			limit = *q.Limit
		}
		return &limit, nil
	}

	// This is a group by query, for which the limit is the upper limit
	// on the maximum number of groups returned. If the limit is nil,
	// no limit is applied.
	if q.Limit == nil {
		return nil, nil
	}
	limit = *q.Limit
	return &limit, nil
}

func (q RawQuery) validateLimit() error {
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

// ParsedQuery represents a validated, sanitized query object produced from a raw query.
type ParsedQuery struct {
	Namespace       string
	StartTimeNanos  int64
	EndTimeNanos    int64
	TimeGranularity time.Duration
	Filters         []FilterList
	GroupBy         []string
	Calculations    []Calculation
	OrderBy         []OrderBy
	Limit           *int
}

// IsRaw returns true if the query is querying raw results (i.e., not grouped), and false otherwise.
func (q *ParsedQuery) IsRaw() bool { return len(q.GroupBy) == 0 }

// IsGrouped returns true if the query is querying grouped results, and false otherwise.
func (q *ParsedQuery) IsGrouped() bool { return !q.IsRaw() }

// FilterList is a list of sanitized filters.
type FilterList struct {
	Filters          []Filter
	FilterCombinator FilterCombinator
}

// Filter is a sanitized filter.
type Filter struct {
	FieldPath []string
	Op        FilterOp
	Value     *field.ValueUnion
}

// Calculation represents a calculation object.
type Calculation struct {
	FieldPath []string
	Op        CalculationOp
}

// OrderBy is a field used for ordering results.
type OrderBy struct {
	FieldType  OrderByFieldType
	FieldIndex int
	FieldPath  []string
	SortOrder  SortOrder
}

// OrderByFieldType is the field type used for ordering.
type OrderByFieldType int

// A list of supported order-by field types.
const (
	UnknownOrderByFieldType OrderByFieldType = iota
	RawField
	GroupByField
	CalculationField
)
