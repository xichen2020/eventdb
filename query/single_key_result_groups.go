package query

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
)

// ForEachSingleKeyResultGroupFn is applied against each result group when iterating over
// result groups.
type ForEachSingleKeyResultGroupFn func(k field.ValueUnion, v calculation.ResultArray) bool

type lenFn func() int
type getOrInsertSingleKeyFn func(k *field.ValueUnion) (calculation.ResultArray, InsertionStatus)
type mergeSingleKeyGroupInPlaceFn func(other *SingleKeyResultGroups)
type marshalJSONFn func(numGroups int, topNRequired bool) ([]byte, error)
type trimToTopNFn func(targetSize int)

// SingleKeyResultGroups stores the result mappings keyed on values from a single field
// whose values are of the same type.
type SingleKeyResultGroups struct {
	keyType                      field.ValueType
	resultArrayProtoType         calculation.ResultArray // Result array to create new result arrays from
	sizeLimit                    int
	lenFn                        lenFn
	getOrInsertFn                getOrInsertSingleKeyFn
	mergeInPlaceFn               mergeSingleKeyGroupInPlaceFn
	marshalJSONFn                marshalJSONFn
	trimToTopNFn                 trimToTopNFn
	boolGroupReverseLessThanFn   boolResultGroupLessThanFn
	intGroupReverseLessThanFn    intResultGroupLessThanFn
	doubleGroupReverseLessThanFn doubleResultGroupLessThanFn
	stringGroupReverseLessThanFn stringResultGroupLessThanFn
	timeGroupReverseLessThanFn   timeResultGroupLessThanFn

	nullResults   calculation.ResultArray
	boolResults   map[bool]calculation.ResultArray
	intResults    map[int]calculation.ResultArray
	doubleResults map[float64]calculation.ResultArray
	stringResults map[string]calculation.ResultArray
	timeResults   map[int64]calculation.ResultArray

	topNBools   *topNBools
	topNInts    *topNInts
	topNDoubles *topNDoubles
	topNStrings *topNStrings
	topNTimes   *topNTimes
}

// NewSingleKeyResultGroups creates a new single key result groups.
func NewSingleKeyResultGroups(
	keyType field.ValueType,
	resultArrayProtoType calculation.ResultArray,
	orderBy []OrderBy,
	sizeLimit int,
	initCapacity int,
) (*SingleKeyResultGroups, error) {
	m := &SingleKeyResultGroups{
		keyType:              keyType,
		resultArrayProtoType: resultArrayProtoType,
		sizeLimit:            sizeLimit,
	}

	var err error
	switch keyType {
	case field.NullType:
		m.lenFn = m.getNullLen
		m.getOrInsertFn = m.getOrInsertNull
		m.mergeInPlaceFn = m.mergeNullGroups
		m.marshalJSONFn = m.marshalJSONNullGroups
		m.trimToTopNFn = m.trimNullToTopN
	case field.BoolType:
		m.boolResults = make(map[bool]calculation.ResultArray, initCapacity)
		m.lenFn = m.getBoolLen
		m.getOrInsertFn = m.getOrInsertBool
		m.mergeInPlaceFn = m.mergeBoolGroups
		m.marshalJSONFn = m.marshalJSONBoolGroups
		m.trimToTopNFn = m.trimBoolToTopN
		m.boolGroupReverseLessThanFn, err = newBoolResultGroupReverseLessThanFn(orderBy)
	case field.IntType:
		m.intResults = make(map[int]calculation.ResultArray, initCapacity)
		m.lenFn = m.getIntLen
		m.getOrInsertFn = m.getOrInsertInt
		m.mergeInPlaceFn = m.mergeIntGroups
		m.marshalJSONFn = m.marshalJSONIntGroups
		m.trimToTopNFn = m.trimIntToTopN
		m.intGroupReverseLessThanFn, err = newIntResultGroupReverseLessThanFn(orderBy)
	case field.DoubleType:
		m.doubleResults = make(map[float64]calculation.ResultArray, initCapacity)
		m.lenFn = m.getDoubleLen
		m.getOrInsertFn = m.getOrInsertDouble
		m.mergeInPlaceFn = m.mergeDoubleGroups
		m.marshalJSONFn = m.marshalJSONDoubleGroups
		m.trimToTopNFn = m.trimDoubleToTopN
		m.doubleGroupReverseLessThanFn, err = newDoubleResultGroupReverseLessThanFn(orderBy)
	case field.StringType:
		m.stringResults = make(map[string]calculation.ResultArray, initCapacity)
		m.lenFn = m.getStringLen
		m.getOrInsertFn = m.getOrInsertString
		m.mergeInPlaceFn = m.mergeStringGroups
		m.marshalJSONFn = m.marshalJSONStringGroups
		m.trimToTopNFn = m.trimStringToTopN
		m.stringGroupReverseLessThanFn, err = newStringResultGroupReverseLessThanFn(orderBy)
	case field.TimeType:
		m.timeResults = make(map[int64]calculation.ResultArray, initCapacity)
		m.lenFn = m.getTimeLen
		m.getOrInsertFn = m.getOrInsertTime
		m.mergeInPlaceFn = m.mergeTimeGroups
		m.marshalJSONFn = m.marshalJSONTimeGroups
		m.trimToTopNFn = m.trimTimeToTopN
		m.timeGroupReverseLessThanFn, err = newTimeResultGroupReverseLessThanFn(orderBy)
	default:
		err = fmt.Errorf("unknown key type %v", keyType)
	}

	if err != nil {
		return nil, err
	}
	return m, nil
}

// Len returns the number of keys in the group.
func (m *SingleKeyResultGroups) Len() int { return m.lenFn() }

// GetOrInsertNoCheck gets the calculation result array from the result for a given key.
// - If the key exists, the existing result array is returned with `Existent`.
// - If the key does not exist, and the size limit hasn't been reached yet, the key is
//   inserted into the map along with a new calculation result array, and `Inserted`.
// - If the key does not exist, and the size limit has already been reached, the key is
//   not inserted, a nil result array is returned with `RejectedDueToLimit`.
//
// NB: No check is performed to ensure the incoming key is not null and has the same type as
// that associated with the map for performance reasons.
// NB: The caller should guarantee the key to insert is not null and guaranteed to have the
// same type as that in the result map.
func (m *SingleKeyResultGroups) GetOrInsertNoCheck(
	key *field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	return m.getOrInsertFn(key)
}

// MergeInPlace merges the other result gruops into the current groups in place.
// The other result groups become invalid after the merge.
// Precondition: The two result groups collect results for the same query, and
// as such both result groups have the same key type and are under the same size limit.
func (m *SingleKeyResultGroups) MergeInPlace(other *SingleKeyResultGroups) {
	if other.Len() == 0 {
		return
	}
	if m.Len() == 0 {
		*m = *other
		other.Clear()
		return
	}
	m.mergeInPlaceFn(other)
	other.Clear()
}

// MarshalJSON marshals `numGroups` result groups as a JSON object.
// If `topNRequired` is true, top N groups are selected based on the corresponding
// `ReverseLessThanFn`. Otherwise, an arbitrary set of groups is selected.
func (m *SingleKeyResultGroups) MarshalJSON(numGroups int, topNRequired bool) ([]byte, error) {
	if numGroups <= 0 {
		return nil, nil
	}
	return m.marshalJSONFn(numGroups, topNRequired)
}

// Clear clears the result groups.
func (m *SingleKeyResultGroups) Clear() {
	m.resultArrayProtoType = nil
	m.lenFn = nil
	m.getOrInsertFn = nil
	m.mergeInPlaceFn = nil
	m.trimToTopNFn = nil
	m.boolGroupReverseLessThanFn = nil
	m.intGroupReverseLessThanFn = nil
	m.doubleGroupReverseLessThanFn = nil
	m.stringGroupReverseLessThanFn = nil
	m.timeGroupReverseLessThanFn = nil
	m.nullResults = nil
	m.boolResults = nil
	m.intResults = nil
	m.doubleResults = nil
	m.stringResults = nil
	m.timeResults = nil
	m.topNBools = nil
	m.topNInts = nil
	m.topNDoubles = nil
	m.topNStrings = nil
	m.topNTimes = nil
}

// trimToTopN trims the number of result groups to the target size.
func (m *SingleKeyResultGroups) trimToTopN(targetSize int) {
	m.trimToTopNFn(targetSize)
}

func (m *SingleKeyResultGroups) getNullLen() int {
	if m.nullResults == nil {
		return 0
	}
	return 1
}

func (m *SingleKeyResultGroups) getBoolLen() int   { return len(m.boolResults) }
func (m *SingleKeyResultGroups) getIntLen() int    { return len(m.intResults) }
func (m *SingleKeyResultGroups) getDoubleLen() int { return len(m.doubleResults) }
func (m *SingleKeyResultGroups) getStringLen() int { return len(m.stringResults) }
func (m *SingleKeyResultGroups) getTimeLen() int   { return len(m.timeResults) }

func (m *SingleKeyResultGroups) getOrInsertNull(
	*field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	if m.nullResults != nil {
		return m.nullResults, Existent
	}
	if m.sizeLimit < 1 {
		return nil, RejectedDueToLimit
	}
	m.nullResults = m.resultArrayProtoType
	return m.nullResults, Inserted
}

func (m *SingleKeyResultGroups) getOrInsertBool(
	key *field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	v := key.BoolVal
	arr, exists := m.boolResults[v]
	if exists {
		return arr, Existent
	}
	if len(m.boolResults) >= m.sizeLimit {
		return nil, RejectedDueToLimit
	}
	arr = m.resultArrayProtoType.New()
	m.boolResults[v] = arr
	return arr, Inserted
}

func (m *SingleKeyResultGroups) getOrInsertInt(
	key *field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	v := key.IntVal
	arr, exists := m.intResults[v]
	if exists {
		return arr, Existent
	}
	if len(m.intResults) >= m.sizeLimit {
		return nil, RejectedDueToLimit
	}
	arr = m.resultArrayProtoType.New()
	m.intResults[v] = arr
	return arr, Inserted
}

func (m *SingleKeyResultGroups) getOrInsertDouble(
	key *field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	v := key.DoubleVal
	arr, exists := m.doubleResults[v]
	if exists {
		return arr, Existent
	}
	if len(m.doubleResults) >= m.sizeLimit {
		return nil, RejectedDueToLimit
	}
	arr = m.resultArrayProtoType.New()
	m.doubleResults[v] = arr
	return arr, Inserted
}

func (m *SingleKeyResultGroups) getOrInsertString(
	key *field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	v := key.StringVal
	arr, exists := m.stringResults[v]
	if exists {
		return arr, Existent
	}
	if len(m.stringResults) >= m.sizeLimit {
		return nil, RejectedDueToLimit
	}
	arr = m.resultArrayProtoType.New()
	m.stringResults[v] = arr
	return arr, Inserted
}

func (m *SingleKeyResultGroups) getOrInsertTime(
	key *field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	v := key.TimeNanosVal
	arr, exists := m.timeResults[v]
	if exists {
		return arr, Existent
	}
	if len(m.timeResults) >= m.sizeLimit {
		return nil, RejectedDueToLimit
	}
	arr = m.resultArrayProtoType.New()
	m.timeResults[v] = arr
	return arr, Inserted
}

func (m *SingleKeyResultGroups) mergeNullGroups(other *SingleKeyResultGroups) {
	if len(other.nullResults) == 0 {
		return
	}
	if len(m.nullResults) == 0 {
		m.nullResults = other.nullResults
		return
	}
	m.nullResults.MergeInPlace(other.nullResults)
}

func (m *SingleKeyResultGroups) mergeBoolGroups(other *SingleKeyResultGroups) {
	if len(other.boolResults) == 0 {
		return
	}
	if len(m.boolResults) == 0 {
		m.boolResults = other.boolResults
		return
	}
	for k, v := range other.boolResults {
		currVal, exists := m.boolResults[k]
		if exists {
			currVal.MergeInPlace(v)
			continue
		}
		// About to insert a new group.
		if len(m.boolResults) >= m.sizeLimit {
			// Limit reached, do nothing.
			continue
		}
		m.boolResults[k] = v
	}
}

func (m *SingleKeyResultGroups) mergeIntGroups(other *SingleKeyResultGroups) {
	if len(other.intResults) == 0 {
		return
	}
	if len(m.intResults) == 0 {
		m.intResults = other.intResults
		return
	}
	for k, v := range other.intResults {
		currVal, exists := m.intResults[k]
		if exists {
			currVal.MergeInPlace(v)
			continue
		}
		// About to insert a new group.
		if len(m.intResults) >= m.sizeLimit {
			// Limit reached, do nothing.
			continue
		}
		m.intResults[k] = v
	}
}

func (m *SingleKeyResultGroups) mergeDoubleGroups(other *SingleKeyResultGroups) {
	if len(other.doubleResults) == 0 {
		return
	}
	if len(m.doubleResults) == 0 {
		m.doubleResults = other.doubleResults
		return
	}
	for k, v := range other.doubleResults {
		currVal, exists := m.doubleResults[k]
		if exists {
			currVal.MergeInPlace(v)
			continue
		}
		// About to insert a new group.
		if len(m.doubleResults) >= m.sizeLimit {
			// Limit reached, do nothing.
			continue
		}
		m.doubleResults[k] = v
	}
}

func (m *SingleKeyResultGroups) mergeStringGroups(other *SingleKeyResultGroups) {
	if len(other.stringResults) == 0 {
		return
	}
	if len(m.stringResults) == 0 {
		m.stringResults = other.stringResults
		return
	}
	for k, v := range other.stringResults {
		currVal, exists := m.stringResults[k]
		if exists {
			currVal.MergeInPlace(v)
			continue
		}
		// About to insert a new group.
		if len(m.stringResults) >= m.sizeLimit {
			// Limit reached, do nothing.
			continue
		}
		m.stringResults[k] = v
	}
}

func (m *SingleKeyResultGroups) mergeTimeGroups(other *SingleKeyResultGroups) {
	if len(other.timeResults) == 0 {
		return
	}
	if len(m.timeResults) == 0 {
		m.timeResults = other.timeResults
		return
	}
	for k, v := range other.timeResults {
		currVal, exists := m.timeResults[k]
		if exists {
			currVal.MergeInPlace(v)
			continue
		}
		// About to insert a new group.
		if len(m.timeResults) >= m.sizeLimit {
			// Limit reached, do nothing.
			continue
		}
		m.timeResults[k] = v
	}
}

func (m *SingleKeyResultGroups) marshalJSONNullGroups(numGroups int, _ bool) ([]byte, error) {
	if numGroups <= 0 {
		return nil, nil
	}
	groups := nullResultGruopsJSON{
		Groups: []nullResultGruop{
			{Values: m.nullResults},
		},
	}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONBoolGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	if numGroups <= 0 {
		return nil, nil
	}
	var res []boolResultGroup
	if topNRequired {
		m.computeTopNBoolGroups(numGroups)
		res = m.topNBools.SortInPlace()
	} else {
		res = make([]boolResultGroup, 0, numGroups)
		for k, v := range m.boolResults {
			group := boolResultGroup{Key: k, Values: v}
			res = append(res, group)
		}
	}
	groups := boolResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONIntGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	if numGroups <= 0 {
		return nil, nil
	}
	var res []intResultGroup
	if topNRequired {
		m.computeTopNIntGroups(numGroups)
		res = m.topNInts.SortInPlace()
	} else {
		res = make([]intResultGroup, 0, numGroups)
		for k, v := range m.intResults {
			group := intResultGroup{Key: k, Values: v}
			res = append(res, group)
		}
	}
	groups := intResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONDoubleGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	if numGroups <= 0 {
		return nil, nil
	}
	var res []doubleResultGroup
	if topNRequired {
		m.computeTopNDoubleGroups(numGroups)
		res = m.topNDoubles.SortInPlace()
	} else {
		res = make([]doubleResultGroup, 0, numGroups)
		for k, v := range m.doubleResults {
			group := doubleResultGroup{Key: k, Values: v}
			res = append(res, group)
		}
	}
	groups := doubleResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONStringGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	if numGroups <= 0 {
		return nil, nil
	}
	var res []stringResultGroup
	if topNRequired {
		m.computeTopNStringGroups(numGroups)
		res = m.topNStrings.SortInPlace()
	} else {
		res = make([]stringResultGroup, 0, numGroups)
		for k, v := range m.stringResults {
			group := stringResultGroup{Key: k, Values: v}
			res = append(res, group)
		}
	}
	groups := stringResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONTimeGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	if numGroups <= 0 {
		return nil, nil
	}
	var res []timeResultGroup
	if topNRequired {
		m.computeTopNTimeGroups(numGroups)
		res = m.topNTimes.SortInPlace()
	} else {
		res = make([]timeResultGroup, 0, numGroups)
		for k, v := range m.timeResults {
			group := timeResultGroup{Key: k, Values: v}
			res = append(res, group)
		}
	}
	groups := timeResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

// computeTopNBoolGroups computes the top N bool groups and stores them in `topNBools`.
func (m *SingleKeyResultGroups) computeTopNBoolGroups(targetSize int) {
	if m.topNBools == nil || m.topNBools.Cap() < targetSize {
		m.topNBools = newTopNBools(targetSize, m.boolGroupReverseLessThanFn)
	}
	for k, v := range m.boolResults {
		group := boolResultGroup{Key: k, Values: v}
		m.topNBools.Add(group, boolAddOptions{})
	}
}

// computeTopNIntGroups computes the top N int groups and stores them in `topNInts`.
func (m *SingleKeyResultGroups) computeTopNIntGroups(targetSize int) {
	if m.topNInts == nil || m.topNInts.Cap() < targetSize {
		m.topNInts = newTopNInts(targetSize, m.intGroupReverseLessThanFn)
	}
	for k, v := range m.intResults {
		group := intResultGroup{Key: k, Values: v}
		m.topNInts.Add(group, intAddOptions{})
	}
}

// computeTopNDoubleGroups computes the top N double groups and stores them in `topNDoubles`.
func (m *SingleKeyResultGroups) computeTopNDoubleGroups(targetSize int) {
	if m.topNDoubles == nil || m.topNDoubles.Cap() < targetSize {
		m.topNDoubles = newTopNDoubles(targetSize, m.doubleGroupReverseLessThanFn)
	}
	for k, v := range m.doubleResults {
		group := doubleResultGroup{Key: k, Values: v}
		m.topNDoubles.Add(group, doubleAddOptions{})
	}
}

// computeTopNStringGroups computes the top N string groups and stores them in `topNStrings`.
func (m *SingleKeyResultGroups) computeTopNStringGroups(targetSize int) {
	if m.topNStrings == nil || m.topNStrings.Cap() < targetSize {
		m.topNStrings = newTopNStrings(targetSize, m.stringGroupReverseLessThanFn)
	}
	for k, v := range m.stringResults {
		group := stringResultGroup{Key: k, Values: v}
		m.topNStrings.Add(group, stringAddOptions{})
	}
}

// computeTopNTimeGroups computes the top N time groups and stores them in `topNTimes`.
func (m *SingleKeyResultGroups) computeTopNTimeGroups(targetSize int) {
	if m.topNTimes == nil || m.topNTimes.Cap() < targetSize {
		m.topNTimes = newTopNTimes(targetSize, m.timeGroupReverseLessThanFn)
	}
	for k, v := range m.timeResults {
		group := timeResultGroup{Key: k, Values: v}
		m.topNTimes.Add(group, timeAddOptions{})
	}
}

func (m *SingleKeyResultGroups) trimNullToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}
	m.nullResults = nil
}

func (m *SingleKeyResultGroups) trimBoolToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	m.computeTopNBoolGroups(targetSize)

	// Allocate a new map and insert the top n bools into the map.
	m.boolResults = make(map[bool]calculation.ResultArray, targetSize)
	data := m.topNBools.RawData()
	for i := 0; i < len(data); i++ {
		m.boolResults[data[i].Key] = data[i].Values
		data[i] = emptyBoolResultGroup
	}
	m.topNBools.Reset()
}

func (m *SingleKeyResultGroups) trimIntToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	m.computeTopNIntGroups(targetSize)

	// Allocate a new map and insert the top n ints into the map.
	m.intResults = make(map[int]calculation.ResultArray, targetSize)
	data := m.topNInts.RawData()
	for i := 0; i < len(data); i++ {
		m.intResults[data[i].Key] = data[i].Values
		data[i] = emptyIntResultGroup
	}
	m.topNInts.Reset()
}

func (m *SingleKeyResultGroups) trimDoubleToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	m.computeTopNDoubleGroups(targetSize)

	// Allocate a new map and insert the top n doubles into the map.
	m.doubleResults = make(map[float64]calculation.ResultArray, targetSize)
	data := m.topNDoubles.RawData()
	for i := 0; i < len(data); i++ {
		m.doubleResults[data[i].Key] = data[i].Values
		data[i] = emptyDoubleResultGroup
	}
	m.topNDoubles.Reset()
}

func (m *SingleKeyResultGroups) trimStringToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	m.computeTopNStringGroups(targetSize)

	// Allocate a new map and insert the top n strings into the map.
	m.stringResults = make(map[string]calculation.ResultArray, targetSize)
	data := m.topNStrings.RawData()
	for i := 0; i < len(data); i++ {
		m.stringResults[data[i].Key] = data[i].Values
		data[i] = emptyStringResultGroup
	}
	m.topNStrings.Reset()
}

func (m *SingleKeyResultGroups) trimTimeToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	m.computeTopNTimeGroups(targetSize)

	// Allocate a new map and insert the top n times into the map.
	m.timeResults = make(map[int64]calculation.ResultArray, targetSize)
	data := m.topNTimes.RawData()
	for i := 0; i < len(data); i++ {
		m.timeResults[data[i].Key] = data[i].Values
		data[i] = emptyTimeResultGroup
	}
	m.topNTimes.Reset()
}
