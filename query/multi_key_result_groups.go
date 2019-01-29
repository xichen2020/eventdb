package query

import (
	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
)

// MultiKeyResultGroups stores the result mappings keyed on an array of values
// from multiple fields.
type MultiKeyResultGroups struct {
	resultArrayProtoType   calculation.ResultArray // Result array to create new result arrays from
	groupReverseLessThanFn multiKeyResultGroupLessThanFn
	sizeLimit              int

	results    *ValuesResultArrayHash
	topNGroups *topNMultiKeyResultGroup
}

// NewMultiKeyResultGroups creates a new multi key result groups object.
func NewMultiKeyResultGroups(
	resultArrayProtoType calculation.ResultArray,
	orderBy []OrderBy,
	sizeLimit int,
	initCapacity int,
) (*MultiKeyResultGroups, error) {
	groupReverseLessThanFn, err := newMultiKeyResultGroupReverseLessThanFn(orderBy)
	if err != nil {
		return nil, err
	}
	return &MultiKeyResultGroups{
		resultArrayProtoType:   resultArrayProtoType,
		groupReverseLessThanFn: groupReverseLessThanFn,
		sizeLimit:              sizeLimit,
		results:                NewValuesResultArrayMap(initCapacity),
	}, nil
}

// Len returns the number of keys in the group.
func (m *MultiKeyResultGroups) Len() int { return m.results.Len() }

// GetOrInsert gets the calculation result array from the result for a given key.
// - If the key exists, the existing result array is returned with `Existent`.
// - If the key does not exist, and the size limit hasn't been reached yet, the key is
//   inserted into the map along with a new calculation result array, and `Inserted`.
// - If the key does not exist, and the size limit has already been reached, the key is
//   not inserted, a nil result array is returned with `RejectedDueToLimit`.
// NB(xichen): The key is cloned when being inserted into the map.
func (m *MultiKeyResultGroups) GetOrInsert(
	key []field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	arr, exists := m.results.Get(key)
	if exists {
		return arr, Existent
	}
	if m.results.Len() >= m.sizeLimit {
		return nil, RejectedDueToLimit
	}
	arr = m.resultArrayProtoType.New()
	m.results.Set(key, arr)
	return arr, Inserted
}

// MergeInPlace merges the other result gruops into the current groups in place.
// The other result groups become invalid after the merge.
// Precondition: The two result groups collect results for the same query, and
// both result groups are under the same size limit.
func (m *MultiKeyResultGroups) MergeInPlace(other *MultiKeyResultGroups) {
	if other == nil || other.results == nil {
		return
	}
	if m.results == nil {
		m.results = other.results
		m.topNGroups = other.topNGroups
		other.Clear()
		return
	}
	// The other result groups own its keys and as such no need to copy.
	setOpts := SetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	}
	iter := other.results.Iter()
	for _, entry := range iter {
		key, value := entry.Key(), entry.Value()
		currVal, exists := m.results.Get(key)
		if exists {
			currVal.MergeInPlace(value)
			continue
		}
		// About to insert a new group.
		if m.results.Len() >= m.sizeLimit {
			// Limit reached, do nothing.
			continue
		}
		m.results.SetUnsafe(key, value, setOpts)
	}
	other.Clear()
}

// Clear clears the result groups.
func (m *MultiKeyResultGroups) Clear() {
	m.resultArrayProtoType = nil
	m.groupReverseLessThanFn = nil
	m.results = nil
	m.topNGroups = nil
}

// trimToTopN trims the number of result groups to the target size.
// Precondition: `m.groupReverseLessThanFn` is not nil.
func (m *MultiKeyResultGroups) trimToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	if m.topNGroups == nil || m.topNGroups.Cap() < targetSize {
		m.topNGroups = newTopNMultiKeyResultGroup(targetSize, m.groupReverseLessThanFn)
	}
	iter := m.results.Iter()
	for _, entry := range iter {
		group := multiKeyResultGroup{key: entry.Key(), value: entry.Value()}
		m.topNGroups.Add(group, multiKeyResultGroupAddOptions{})
	}

	// Allocate a new map and insert the top n groups into the map.
	m.results = NewValuesResultArrayMap(targetSize)
	setOpts := SetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	}
	data := m.topNGroups.RawData()
	for i := 0; i < len(data); i++ {
		m.results.SetUnsafe(data[i].key, data[i].value, setOpts)
		data[i] = emptyMultiKeyResultGroup
	}
	m.topNGroups.Reset()
}

// multiKeyResultGroup is a multi-key result group.
type multiKeyResultGroup struct {
	key   field.Values
	value calculation.ResultArray
}

var emptyMultiKeyResultGroup multiKeyResultGroup

type multiKeyResultGroupLessThanFn func(v1, v2 multiKeyResultGroup) bool

func newMultiKeyResultGroupReverseLessThanFn(orderBy []OrderBy) (multiKeyResultGroupLessThanFn, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}
	// NB(xichen): Eagerly compute the comparison functions so they are readily available
	// when comparing result groups, which is a reasonable memory-perf tradeoff as the
	// group comparison function is usually called against a large number of groups.
	var (
		compareFieldValueFns = make([]field.ValueCompareFn, 0, len(orderBy))
		compareCalcValueFns  = make([]calculation.ValueCompareFn, 0, len(orderBy))
	)
	for _, ob := range orderBy {
		fvFn, err := ob.SortOrder.CompareFieldValueFn()
		if err != nil {
			return nil, err
		}
		compareFieldValueFns = append(compareFieldValueFns, fvFn)

		cvFn, err := ob.SortOrder.CompareCalcValueFn()
		if err != nil {
			return nil, err
		}
		compareCalcValueFns = append(compareCalcValueFns, cvFn)
	}
	groupReverseLessThanFn := func(g1, g2 multiKeyResultGroup) bool {
		for i, ob := range orderBy {
			var res int
			if ob.FieldType == GroupByField {
				res = compareFieldValueFns[i](g1.key[ob.FieldIndex], g2.key[ob.FieldIndex])
			} else {
				res = compareCalcValueFns[i](g1.value[ob.FieldIndex].Value(), g2.value[ob.FieldIndex].Value())
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
