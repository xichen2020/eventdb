package query

import (
	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/x/compare"
	"github.com/xichen2020/eventdb/x/safe"
)

type nullResultGroup struct {
	Key    *string                 `json:"key"` // This should always be a nil pointer to represent null key
	Values calculation.ResultArray `json:"values"`
}

type nullResultGroupsJSON struct {
	Groups []nullResultGroup `json:"groups"`
}

type boolResultGroup struct {
	Key    bool                    `json:"key"`
	Values calculation.ResultArray `json:"values"`
}

var emptyBoolResultGroup boolResultGroup

type boolResultGroupLessThanFn func(v1, v2 boolResultGroup) bool

type boolResultGroupsJSON struct {
	Groups []boolResultGroup `json:"groups"`
}

func newBoolResultGroupReverseLessThanFn(orderBy []OrderBy) (boolResultGroupLessThanFn, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}
	// NB(xichen): Eagerly compute the comparison functions so they are readily available
	// when comparing result groups, which is a reasonable memory-perf tradeoff as the
	// group comparison function is usually called against a large number of groups.
	var (
		compareBoolFns      = make([]compare.BoolCompareFn, 0, len(orderBy))
		compareCalcValueFns = make([]calculation.ValueCompareFn, 0, len(orderBy))
	)
	for _, ob := range orderBy {
		fvFn, err := ob.SortOrder.CompareBoolFn()
		if err != nil {
			return nil, err
		}
		compareBoolFns = append(compareBoolFns, fvFn)

		cvFn, err := ob.SortOrder.CompareCalcValueFn()
		if err != nil {
			return nil, err
		}
		compareCalcValueFns = append(compareCalcValueFns, cvFn)
	}
	groupReverseLessThanFn := func(g1, g2 boolResultGroup) bool {
		for i, ob := range orderBy {
			var res int
			if ob.FieldType == GroupByField {
				res = compareBoolFns[i](g1.Key, g2.Key)
			} else {
				res = compareCalcValueFns[i](g1.Values[ob.FieldIndex].Value(), g2.Values[ob.FieldIndex].Value())
			}
			if res > 0 {
				return true
			}
			if res < 0 {
				return false
			}
		}
		return true
	}
	return groupReverseLessThanFn, nil
}

type intResultGroup struct {
	Key    int                     `json:"key"`
	Values calculation.ResultArray `json:"values"`
}

var emptyIntResultGroup intResultGroup

type intResultGroupLessThanFn func(v1, v2 intResultGroup) bool

type intResultGroupsJSON struct {
	Groups []intResultGroup `json:"groups"`
}

func newIntResultGroupReverseLessThanFn(orderBy []OrderBy) (intResultGroupLessThanFn, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}
	// NB(xichen): Eagerly compute the comparison functions so they are readily available
	// when comparing result groups, which is a reasonable memory-perf tradeoff as the
	// group comparison function is usually called against a large number of groups.
	var (
		compareIntFns       = make([]compare.IntCompareFn, 0, len(orderBy))
		compareCalcValueFns = make([]calculation.ValueCompareFn, 0, len(orderBy))
	)
	for _, ob := range orderBy {
		fvFn, err := ob.SortOrder.CompareIntFn()
		if err != nil {
			return nil, err
		}
		compareIntFns = append(compareIntFns, fvFn)

		cvFn, err := ob.SortOrder.CompareCalcValueFn()
		if err != nil {
			return nil, err
		}
		compareCalcValueFns = append(compareCalcValueFns, cvFn)
	}
	groupReverseLessThanFn := func(g1, g2 intResultGroup) bool {
		for i, ob := range orderBy {
			var res int
			if ob.FieldType == GroupByField {
				res = compareIntFns[i](g1.Key, g2.Key)
			} else {
				res = compareCalcValueFns[i](g1.Values[ob.FieldIndex].Value(), g2.Values[ob.FieldIndex].Value())
			}
			if res > 0 {
				return true
			}
			if res < 0 {
				return false
			}
		}
		return true
	}
	return groupReverseLessThanFn, nil
}

type doubleResultGroup struct {
	Key    float64                 `json:"key"`
	Values calculation.ResultArray `json:"values"`
}

var emptyDoubleResultGroup doubleResultGroup

type doubleResultGroupLessThanFn func(v1, v2 doubleResultGroup) bool

type doubleResultGroupsJSON struct {
	Groups []doubleResultGroup `json:"groups"`
}

func newDoubleResultGroupReverseLessThanFn(orderBy []OrderBy) (doubleResultGroupLessThanFn, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}
	// NB(xichen): Eagerly compute the comparison functions so they are readily available
	// when comparing result groups, which is a reasonable memory-perf tradeoff as the
	// group comparison function is usually called against a large number of groups.
	var (
		compareDoubleFns    = make([]compare.DoubleCompareFn, 0, len(orderBy))
		compareCalcValueFns = make([]calculation.ValueCompareFn, 0, len(orderBy))
	)
	for _, ob := range orderBy {
		fvFn, err := ob.SortOrder.CompareDoubleFn()
		if err != nil {
			return nil, err
		}
		compareDoubleFns = append(compareDoubleFns, fvFn)

		cvFn, err := ob.SortOrder.CompareCalcValueFn()
		if err != nil {
			return nil, err
		}
		compareCalcValueFns = append(compareCalcValueFns, cvFn)
	}
	groupReverseLessThanFn := func(g1, g2 doubleResultGroup) bool {
		for i, ob := range orderBy {
			var res int
			if ob.FieldType == GroupByField {
				res = compareDoubleFns[i](g1.Key, g2.Key)
			} else {
				res = compareCalcValueFns[i](g1.Values[ob.FieldIndex].Value(), g2.Values[ob.FieldIndex].Value())
			}
			if res > 0 {
				return true
			}
			if res < 0 {
				return false
			}
		}
		return true
	}
	return groupReverseLessThanFn, nil
}

type bytesResultGroup struct {
	Key    string                  `json:"key"`
	Values calculation.ResultArray `json:"values"`
}

var emptyBytesResultGroup bytesResultGroup

type bytesResultGroupLessThanFn func(v1, v2 bytesResultGroup) bool

type bytesResultGroupsJSON struct {
	Groups []bytesResultGroup `json:"groups"`
}

func newBytesResultGroupReverseLessThanFn(orderBy []OrderBy) (bytesResultGroupLessThanFn, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}
	// NB(xichen): Eagerly compute the comparison functions so they are readily available
	// when comparing result groups, which is a reasonable memory-perf tradeoff as the
	// group comparison function is usually called against a large number of groups.
	var (
		compareBytesFns     = make([]compare.BytesCompareFn, 0, len(orderBy))
		compareCalcValueFns = make([]calculation.ValueCompareFn, 0, len(orderBy))
	)
	for _, ob := range orderBy {
		fvFn, err := ob.SortOrder.CompareBytesFn()
		if err != nil {
			return nil, err
		}
		compareBytesFns = append(compareBytesFns, fvFn)

		cvFn, err := ob.SortOrder.CompareCalcValueFn()
		if err != nil {
			return nil, err
		}
		compareCalcValueFns = append(compareCalcValueFns, cvFn)
	}
	groupReverseLessThanFn := func(g1, g2 bytesResultGroup) bool {
		g1Key, g2Key := safe.ToBytes(g1.Key), safe.ToBytes(g2.Key)
		for i, ob := range orderBy {
			var res int
			if ob.FieldType == GroupByField {
				res = compareBytesFns[i](g1Key, g2Key)
			} else {
				res = compareCalcValueFns[i](g1.Values[ob.FieldIndex].Value(), g2.Values[ob.FieldIndex].Value())
			}
			if res > 0 {
				return true
			}
			if res < 0 {
				return false
			}
		}
		return true
	}
	return groupReverseLessThanFn, nil
}

type timeResultGroup struct {
	Key    int64                   `json:"key"`
	Values calculation.ResultArray `json:"values"`
}

var emptyTimeResultGroup timeResultGroup

type timeResultGroupLessThanFn func(v1, v2 timeResultGroup) bool

type timeResultGroupsJSON struct {
	Groups []timeResultGroup `json:"groups"`
}

func newTimeResultGroupReverseLessThanFn(orderBy []OrderBy) (timeResultGroupLessThanFn, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}
	// NB(xichen): Eagerly compute the comparison functions so they are readily available
	// when comparing result groups, which is a reasonable memory-perf tradeoff as the
	// group comparison function is usually called against a large number of groups.
	var (
		compareTimeFns      = make([]compare.TimeCompareFn, 0, len(orderBy))
		compareCalcValueFns = make([]calculation.ValueCompareFn, 0, len(orderBy))
	)
	for _, ob := range orderBy {
		fvFn, err := ob.SortOrder.CompareTimeFn()
		if err != nil {
			return nil, err
		}
		compareTimeFns = append(compareTimeFns, fvFn)

		cvFn, err := ob.SortOrder.CompareCalcValueFn()
		if err != nil {
			return nil, err
		}
		compareCalcValueFns = append(compareCalcValueFns, cvFn)
	}
	groupReverseLessThanFn := func(g1, g2 timeResultGroup) bool {
		for i, ob := range orderBy {
			var res int
			if ob.FieldType == GroupByField {
				res = compareTimeFns[i](g1.Key, g2.Key)
			} else {
				res = compareCalcValueFns[i](g1.Values[ob.FieldIndex].Value(), g2.Values[ob.FieldIndex].Value())
			}
			if res > 0 {
				return true
			}
			if res < 0 {
				return false
			}
		}
		return true
	}
	return groupReverseLessThanFn, nil
}
