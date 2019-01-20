package query

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/x/convert"
	"github.com/xichen2020/eventdb/x/hash"

	xtime "github.com/m3db/m3x/time"
)

const (
	defaultTimeGranularity       = time.Second
	defaultTimeUnit              = TimeUnit(xtime.Second)
	defaultFilterCombinator      = filter.And
	defaultRawQuerySizeLimit     = 100
	defaultGroupedQuerySizeLimit = 10
	defaultOrderBySortOrder      = Ascending
)

var (
	errNoNamespaceInQuery                  = errors.New("no namespace provided in query")
	errStartAndEndRequiredWithNoRange      = errors.New("both start time and end time required when no time range is specified")
	errNoFieldInFilter                     = errors.New("no field specified in query filter")
	errNoFieldInOrderByForRawQuery         = errors.New("no field specified for raw query")
	errNoFieldOrOpInOrderByForGroupedQuery = errors.New("no field or op specified in order by for grouped query")
	errCalculationsWithNoGroups            = errors.New("calculations provided with no groups")
)

// RawQuery represents a raw document query useful for serializing/deserializing in JSON.
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

// RawCalculation represents a raw calculation object.
type RawCalculation struct {
	Field *string        `json:"field"`
	Op    calculation.Op `json:"op"`
}

// RawOrderBy represents a list of criteria for ordering results.
type RawOrderBy struct {
	Field *string         `json:"field"`
	Op    *calculation.Op `json:"op"`
	Order *SortOrder      `json:"order"`
}

// ParseOptions provide a set of options for parsing a raw query.
type ParseOptions struct {
	FieldPathSeparator    byte
	FieldHashFn           hash.StringArrayHashFn
	TimestampFieldPath    []string
	RawDocSourceFieldPath []string
}

// Parse parses the raw query, returning any errors encountered.
func (q *RawQuery) Parse(opts ParseOptions) (ParsedQuery, error) {
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

	sq.GroupBy = q.parseGroupBy(opts)

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

func (q *RawQuery) parseGroupBy(opts ParseOptions) [][]string {
	if len(q.GroupBy) == 0 {
		return nil
	}
	groupByFieldPaths := make([][]string, 0, len(q.GroupBy))
	for _, gb := range q.GroupBy {
		fieldPaths := parseField(gb, opts.FieldPathSeparator)
		groupByFieldPaths = append(groupByFieldPaths, fieldPaths)
	}
	return groupByFieldPaths
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
	// If the calculation operation is count, the field should be omitted,
	// in which case the number of events containing the corresponding group key
	// is returned. All other operations require the field be specified.
	for _, calc := range q.Calculations {
		if calc.Field != nil && !calc.Op.RequiresField() {
			return fmt.Errorf("field specified %v for calculation op %v", *calc.Field, calc.Op)
		}
		if calc.Field == nil && calc.Op.RequiresField() {
			return fmt.Errorf("no field specified for calculation op %v", calc.Op)
		}
	}
	return nil
}

func (q *RawQuery) parseOrderByList(
	groupBy [][]string,
	calculations []Calculation,
	opts ParseOptions,
) ([]OrderBy, error) {
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

func (q *RawQuery) parseOrderBy(
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
			if calc.Field != nil && rob.Field != nil && calc.Field != rob.Field {
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
func (q *RawQuery) parseLimit() (int, error) {
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
	GroupBy         [][]string
	Calculations    []Calculation
	OrderBy         []OrderBy
	Limit           int

	// Derived fields for raw query.
	// AllowedFieldTypes map[hash.Hash]FieldMeta
	opts             ParseOptions
	valuesLessThanFn field.ValuesLessThanFn
}

// IsRaw returns true if the query is querying raw results (i.e., not grouped), and false otherwise.
func (q *ParsedQuery) IsRaw() bool { return len(q.GroupBy) == 0 }

// IsGrouped returns true if the query is querying grouped results, and false otherwise.
func (q *ParsedQuery) IsGrouped() bool { return !q.IsRaw() }

// RawQuery returns the parsed raw query for raw results.
func (q *ParsedQuery) RawQuery() (ParsedRawQuery, error) {
	return newParsedRawQuery(q)
}

// GroupedQuery returns the parsed grouped query for grouped results.
func (q *ParsedQuery) GroupedQuery() (ParsedGroupedQuery, error) {
	return newParsedGroupedQuery(q)
}

func (q *ParsedQuery) computeDerived() error {
	return q.computeValueCompareFns()
}

func (q *ParsedQuery) computeValueCompareFns() error {
	compareFns := make([]field.ValueCompareFn, 0, len(q.OrderBy))
	for _, ob := range q.OrderBy {
		compareFn, err := ob.SortOrder.CompareFn()
		if err != nil {
			return fmt.Errorf("error determining the value compare fn for sort order %v: %v", ob.SortOrder, err)
		}
		compareFns = append(compareFns, compareFn)
	}
	q.valuesLessThanFn = field.NewValuesLessThanFn(compareFns)
	return nil
}

// FieldMeta contains field metadata.
type FieldMeta struct {
	FieldPath []string

	// Required is true if the field must be present, otherwise empty result is returned.
	// This applies to `GroupBy`, `Calculation`, and `OrderBy` fields.
	IsRequired bool

	// AllowedTypesBySourceIdx contains the set of field types allowed by the query,
	// keyed by the field index in the query clause.
	AllowedTypesBySourceIdx map[int]field.ValueTypeSet
}

// MergeInPlace merges the other field meta into the current field meta.
// Precondition: m.fieldPath == other.fieldPath.
// Precondition: The set of source indices in the two metas don't overlap.
func (m *FieldMeta) MergeInPlace(other FieldMeta) {
	if other.IsRequired {
		// If one of the field metas dictates the field is required, mark the field as so.
		m.IsRequired = true
	}
	for idx, types := range other.AllowedTypesBySourceIdx {
		m.AllowedTypesBySourceIdx[idx] = types
	}
}

// ParsedRawQuery represents a validated, sanitized raw query.
type ParsedRawQuery struct {
	Namespace           string
	StartNanosInclusive int64
	EndNanosExclusive   int64
	Filters             []FilterList
	OrderBy             []OrderBy
	Limit               int
	ValuesLessThanFn    field.ValuesLessThanFn

	// Derived fields.
	ResultLessThanFn        RawResultLessThanFn
	ResultReverseLessThanFn RawResultLessThanFn
	FieldConstraints        map[hash.Hash]FieldMeta // Field constraints inferred from query
}

func newParsedRawQuery(q *ParsedQuery) (ParsedRawQuery, error) {
	rq := ParsedRawQuery{
		Namespace:           q.Namespace,
		StartNanosInclusive: q.StartTimeNanos,
		EndNanosExclusive:   q.EndTimeNanos,
		Filters:             q.Filters,
		OrderBy:             q.OrderBy,
		Limit:               q.Limit,
		ValuesLessThanFn:    q.valuesLessThanFn,
	}
	if err := rq.computeDerived(q.opts); err != nil {
		return ParsedRawQuery{}, err
	}
	return rq, nil
}

// NumFieldsForQuery returns the total number of fields involved in executing the query.
func (q *ParsedRawQuery) NumFieldsForQuery() int {
	numFieldsForQuery := 2 // Timestamp field and raw doc source field
	numFieldsForQuery += q.NumFilters()
	numFieldsForQuery += len(q.OrderBy)
	return numFieldsForQuery
}

// NumFilters returns the number of filters in the query.
func (q *ParsedRawQuery) NumFilters() int {
	numFilters := 0
	for _, f := range q.Filters {
		numFilters += len(f.Filters)
	}
	return numFilters
}

// NewRawResults creates a new raw results from the parsed raw query.
func (q *ParsedRawQuery) NewRawResults() RawResults {
	return RawResults{
		OrderBy:                 q.OrderBy,
		Limit:                   q.Limit,
		ValuesLessThanFn:        q.ValuesLessThanFn,
		ResultLessThanFn:        q.ResultLessThanFn,
		ResultReverseLessThanFn: q.ResultReverseLessThanFn,
	}
}

// computeDerived computes the derived fields.
func (q *ParsedRawQuery) computeDerived(opts ParseOptions) error {
	q.ResultLessThanFn = func(r1, r2 RawResult) bool {
		return q.ValuesLessThanFn(r1.OrderByValues, r2.OrderByValues)
	}
	q.ResultReverseLessThanFn = func(r1, r2 RawResult) bool {
		return !q.ResultLessThanFn(r1, r2)
	}

	fieldConstraints, err := q.computeFieldConstraints(opts)
	if err != nil {
		return err
	}
	q.FieldConstraints = fieldConstraints
	return nil
}

func (q *ParsedRawQuery) computeFieldConstraints(
	opts ParseOptions,
) (map[hash.Hash]FieldMeta, error) {
	// Compute total number of fields involved in executing the query.
	numFieldsForQuery := q.NumFieldsForQuery()

	// Collect fields needed for query execution into a map for deduplciation.
	fieldMap := make(map[hash.Hash]FieldMeta, numFieldsForQuery)

	// Insert timestamp field.
	currIndex := 0
	addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
		FieldPath:  opts.TimestampFieldPath,
		IsRequired: true,
		AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
			currIndex: field.ValueTypeSet{
				field.TimeType: struct{}{},
			},
		},
	})

	// Insert raw doc source field.
	currIndex++
	addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
		FieldPath:  opts.RawDocSourceFieldPath,
		IsRequired: true,
		AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
			currIndex: field.ValueTypeSet{
				field.StringType: struct{}{},
			},
		},
	})

	// Insert filter fields.
	currIndex++
	for _, fl := range q.Filters {
		for _, f := range fl.Filters {
			allowedFieldTypes, err := f.AllowedFieldTypes()
			if err != nil {
				return nil, err
			}
			addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
				FieldPath:  f.FieldPath,
				IsRequired: false,
				AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
					currIndex: allowedFieldTypes,
				},
			})
			currIndex++
		}
	}

	// Insert order by fields.
	for _, ob := range q.OrderBy {
		addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
			FieldPath:  ob.FieldPath,
			IsRequired: true,
			AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
				currIndex: field.OrderableTypes.Clone(),
			},
		})
		currIndex++
	}

	return fieldMap, nil
}

// ParsedGroupedQuery represents a validated, sanitized group query.
type ParsedGroupedQuery struct {
	Namespace           string
	StartNanosInclusive int64
	EndNanosExclusive   int64
	TimeGranularity     time.Duration
	Filters             []FilterList
	GroupBy             [][]string
	Calculations        []Calculation
	OrderBy             []OrderBy
	Limit               int
	ValuesLessThanFn    field.ValuesLessThanFn

	// Derived fields.
	FieldConstraints map[hash.Hash]FieldMeta // Field constraints inferred from query
}

func newParsedGroupedQuery(q *ParsedQuery) (ParsedGroupedQuery, error) {
	gq := ParsedGroupedQuery{
		Namespace:           q.Namespace,
		StartNanosInclusive: q.StartTimeNanos,
		EndNanosExclusive:   q.EndTimeNanos,
		TimeGranularity:     q.TimeGranularity,
		Filters:             q.Filters,
		GroupBy:             q.GroupBy,
		Calculations:        q.Calculations,
		OrderBy:             q.OrderBy,
		Limit:               q.Limit,
		ValuesLessThanFn:    q.valuesLessThanFn,
	}
	if err := gq.computeDerived(q.opts); err != nil {
		return ParsedGroupedQuery{}, err
	}
	return gq, nil
}

// NewGroupedResults creats a new grouped results from the parsed grouped query.
func (q *ParsedGroupedQuery) NewGroupedResults() GroupedResults {
	return GroupedResults{
		OrderBy:          q.OrderBy,
		Limit:            q.Limit,
		ValuesLessThanFn: q.ValuesLessThanFn,
	}
}

// NumFieldsForQuery returns the total number of fields involved in executing the query.
// NB: `OrderBy` fields are covered by either `GroupBy` or `Calculations`
func (q *ParsedGroupedQuery) NumFieldsForQuery() int {
	numFieldsForQuery := 1 // Timestamp field
	numFieldsForQuery += q.NumFilters()
	numFieldsForQuery += len(q.GroupBy)
	for _, calc := range q.Calculations {
		if calc.FieldPath == nil {
			continue
		}
		numFieldsForQuery++
	}
	return numFieldsForQuery
}

// NumFilters returns the number of filters in the query.
func (q *ParsedGroupedQuery) NumFilters() int {
	numFilters := 0
	for _, f := range q.Filters {
		numFilters += len(f.Filters)
	}
	return numFilters
}

func (q *ParsedGroupedQuery) computeDerived(opts ParseOptions) error {
	fieldConstraints, err := q.computeFieldConstraints(opts)
	if err != nil {
		return err
	}
	q.FieldConstraints = fieldConstraints
	return nil
}

func (q *ParsedGroupedQuery) computeFieldConstraints(
	opts ParseOptions,
) (map[hash.Hash]FieldMeta, error) {
	// Compute total number of fields involved in executing the query.
	numFieldsForQuery := q.NumFieldsForQuery()

	// Collect fields needed for query execution into a map for deduplciation.
	fieldMap := make(map[hash.Hash]FieldMeta, numFieldsForQuery)

	// Insert timestamp field.
	currIndex := 0
	addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
		FieldPath:  opts.TimestampFieldPath,
		IsRequired: true,
		AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
			currIndex: field.ValueTypeSet{
				field.TimeType: struct{}{},
			},
		},
	})

	// Insert filter fields.
	currIndex++
	for _, fl := range q.Filters {
		for _, f := range fl.Filters {
			allowedFieldTypes, err := f.AllowedFieldTypes()
			if err != nil {
				return nil, err
			}
			addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
				FieldPath:  f.FieldPath,
				IsRequired: false,
				AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
					currIndex: allowedFieldTypes,
				},
			})
			currIndex++
		}
	}

	// Insert group by fields.
	for _, gb := range q.GroupBy {
		addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
			FieldPath:  gb,
			IsRequired: true,
			AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
				currIndex: field.GroupableTypes.Clone(),
			},
		})
		currIndex++
	}

	// Insert calculation fields.
	for _, calc := range q.Calculations {
		if calc.FieldPath == nil {
			continue
		}
		allowedFieldTypes, err := calc.Op.AllowedTypes()
		if err != nil {
			return nil, err
		}
		// NB(xichen): Restrict the calculation field types for now, but in the future
		// can relax this constraint.
		addQueryFieldToMap(fieldMap, opts.FieldHashFn, FieldMeta{
			FieldPath:  calc.FieldPath,
			IsRequired: true,
			AllowedTypesBySourceIdx: map[int]field.ValueTypeSet{
				currIndex: allowedFieldTypes,
			},
		})
		currIndex++
	}

	return fieldMap, nil
}

// addQueryFieldToMap adds a new query field meta to the existing
// field meta map.
func addQueryFieldToMap(
	fm map[hash.Hash]FieldMeta,
	fieldHashFn hash.StringArrayHashFn,
	newFieldMeta FieldMeta,
) {
	// Do not insert empty fields.
	if len(newFieldMeta.FieldPath) == 0 {
		return
	}
	fieldHash := fieldHashFn(newFieldMeta.FieldPath)
	meta, exists := fm[fieldHash]
	if !exists {
		fm[fieldHash] = newFieldMeta
		return
	}
	meta.MergeInPlace(newFieldMeta)
	fm[fieldHash] = meta
}

// Calculation represents a calculation object.
type Calculation struct {
	FieldPath []string
	Op        calculation.Op
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
