package query

import "time"

// Query represents an event query.
type Query struct {
	// Time range portion of the query.
	StartTime       *int64         `json:"start_time"`
	EndTime         *int64         `json:"end_time"`
	TimeUnit        *TimeUnit      `json:"time_unit"`
	TimeRange       *time.Duration `json:"time_range"`
	TimeGranularity *time.Duration `json:"time_granularity"`

	// Filters.
	Filters []FilterList `json:"filters"`

	// A list of fields to group the results by.
	GroupBy []string `json:"group_by"`

	// A list of calculations to perform within each group.
	// If no groups are specified, the calculations are performed against
	// the entire group.
	Calculations []Calculation `yaml:"calculations"`

	// A list of criteria to order the results by. Each criteria must appear
	// either in the group by list or in the calculations list.
	OrderBy []OrderBy `yaml:"order_by"`

	// Maximum number of results returned.
	Limit *int `yaml:"limit"`
}

// FilterList is a list of filters.
type FilterList struct {
	Filters          []Filter          `json:"filters"`
	FilterCombinator *FilterCombinator `json:"filter_combinator"`
}

// Filter represents a query filter.
type Filter struct {
	Field string      `json:"field"`
	Op    FilterOp    `json:"op"`
	Value interface{} `json:"value"`
}

// Calculation represents a calculation object.
type Calculation struct {
	Field *string       `json:"field"`
	Op    CalculationOp `json:"op"`
}

// OrderBy represents a list of criteria for ordering results.
type OrderBy struct {
	Field *string        `json:"field"`
	Op    *CalculationOp `json:"op"`
	Order *SortOrder     `json:"order"`
}
