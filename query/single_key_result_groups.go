package query

import (
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
)

// ForEachSingleKeyResultGroupFn is applied against each result group when iterating over
// result groups.
type ForEachSingleKeyResultGroupFn func(k field.ValueUnion, v calculation.ResultArray) bool

type lenFn func() int
type getOrInsertSingleKeyFn func(k *field.ValueUnion) (calculation.ResultArray, InsertionStatus)
type forEachSingleKeyGroupFn func(fn ForEachSingleKeyResultGroupFn)
type mergeSingleKeyGroupInPlaceFn func(other *SingleKeyResultGroups)
type trimToTopNFn func(targetSize int)

// SingleKeyResultGroups stores the result mappings keyed on values from a single field
// whose values are of the same type.
type SingleKeyResultGroups struct {
	keyType                      field.ValueType
	resultArrayProtoType         calculation.ResultArray // Result array to create new result arrays from
	sizeLimit                    int
	lenFn                        lenFn
	getOrInsertFn                getOrInsertSingleKeyFn
	forEachGroupFn               forEachSingleKeyGroupFn
	mergeInPlaceFn               mergeSingleKeyGroupInPlaceFn
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

	boolHeap   *boolResultGroupHeap
	intHeap    *intResultGroupHeap
	doubleHeap *doubleResultGroupHeap
	stringHeap *stringResultGroupHeap
	timeHeap   *timeResultGroupHeap
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
		m.forEachGroupFn = m.forEachNullGroup
		m.mergeInPlaceFn = m.mergeNullGroups
		m.trimToTopNFn = m.trimNullToTopN
	case field.BoolType:
		m.boolResults = make(map[bool]calculation.ResultArray, initCapacity)
		m.lenFn = m.getBoolLen
		m.getOrInsertFn = m.getOrInsertBool
		m.forEachGroupFn = m.forEachBoolGroup
		m.mergeInPlaceFn = m.mergeBoolGroups
		m.trimToTopNFn = m.trimBoolToTopN
		m.boolGroupReverseLessThanFn, err = newBoolResultGroupReverseLessThanFn(orderBy)
	case field.IntType:
		m.intResults = make(map[int]calculation.ResultArray, initCapacity)
		m.lenFn = m.getIntLen
		m.getOrInsertFn = m.getOrInsertInt
		m.forEachGroupFn = m.forEachIntGroup
		m.mergeInPlaceFn = m.mergeIntGroups
		m.trimToTopNFn = m.trimIntToTopN
		m.intGroupReverseLessThanFn, err = newIntResultGroupReverseLessThanFn(orderBy)
	case field.DoubleType:
		m.doubleResults = make(map[float64]calculation.ResultArray, initCapacity)
		m.lenFn = m.getDoubleLen
		m.getOrInsertFn = m.getOrInsertDouble
		m.forEachGroupFn = m.forEachDoubleGroup
		m.mergeInPlaceFn = m.mergeDoubleGroups
		m.trimToTopNFn = m.trimDoubleToTopN
		m.doubleGroupReverseLessThanFn, err = newDoubleResultGroupReverseLessThanFn(orderBy)
	case field.StringType:
		m.stringResults = make(map[string]calculation.ResultArray, initCapacity)
		m.lenFn = m.getStringLen
		m.getOrInsertFn = m.getOrInsertString
		m.forEachGroupFn = m.forEachStringGroup
		m.mergeInPlaceFn = m.mergeStringGroups
		m.trimToTopNFn = m.trimStringToTopN
		m.stringGroupReverseLessThanFn, err = newStringResultGroupReverseLessThanFn(orderBy)
	case field.TimeType:
		m.timeResults = make(map[int64]calculation.ResultArray, initCapacity)
		m.lenFn = m.getTimeLen
		m.getOrInsertFn = m.getOrInsertTime
		m.forEachGroupFn = m.forEachTimeGroup
		m.mergeInPlaceFn = m.mergeTimeGroups
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

// ForEach applies the function against each result group, and stops iterating
// as soon as the function returns false.
func (m *SingleKeyResultGroups) ForEach(fn ForEachSingleKeyResultGroupFn) {
	m.forEachGroupFn(fn)
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

// Clear clears the result groups.
func (m *SingleKeyResultGroups) Clear() {
	m.resultArrayProtoType = nil
	m.lenFn = nil
	m.getOrInsertFn = nil
	m.forEachGroupFn = nil
	m.mergeInPlaceFn = nil
	m.nullResults = nil
	m.boolResults = nil
	m.intResults = nil
	m.doubleResults = nil
	m.stringResults = nil
	m.timeResults = nil
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

func (m *SingleKeyResultGroups) forEachNullGroup(fn ForEachSingleKeyResultGroupFn) {
	if m.nullResults == nil {
		return
	}
	fn(field.NullUnion, m.nullResults)
}

func (m *SingleKeyResultGroups) forEachBoolGroup(fn ForEachSingleKeyResultGroupFn) {
	for k, v := range m.boolResults {
		fn(field.NewBoolUnion(k), v)
	}
}

func (m *SingleKeyResultGroups) forEachIntGroup(fn ForEachSingleKeyResultGroupFn) {
	for k, v := range m.intResults {
		fn(field.NewIntUnion(k), v)
	}
}

func (m *SingleKeyResultGroups) forEachDoubleGroup(fn ForEachSingleKeyResultGroupFn) {
	for k, v := range m.doubleResults {
		fn(field.NewDoubleUnion(k), v)
	}
}

func (m *SingleKeyResultGroups) forEachStringGroup(fn ForEachSingleKeyResultGroupFn) {
	for k, v := range m.stringResults {
		fn(field.NewStringUnion(k), v)
	}
}

func (m *SingleKeyResultGroups) forEachTimeGroup(fn ForEachSingleKeyResultGroupFn) {
	for k, v := range m.timeResults {
		fn(field.NewTimeUnion(k), v)
	}
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
	if m.boolHeap == nil || m.boolHeap.Cap() < targetSize {
		m.boolHeap = newBoolResultGroupHeap(targetSize, m.boolGroupReverseLessThanFn)
	}
	for k, v := range m.boolResults {
		group := boolResultGroup{key: k, value: v}
		if m.boolHeap.Len() < targetSize {
			m.boolHeap.Push(group)
			continue
		}
		if min := m.boolHeap.Min(); !m.boolGroupReverseLessThanFn(min, group) {
			continue
		}
		m.boolHeap.Pop()
		m.boolHeap.Push(group)
	}

	// Allocate a new map and insert the boolHeap into the map.
	m.boolResults = make(map[bool]calculation.ResultArray, targetSize)
	data := m.boolHeap.RawData()
	for i := 0; i < len(data); i++ {
		m.boolResults[data[i].key] = data[i].value
		data[i] = emptyBoolResultGroup
	}
	m.boolHeap.Reset()
}

func (m *SingleKeyResultGroups) trimIntToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	if m.intHeap == nil || m.intHeap.Cap() < targetSize {
		m.intHeap = newIntResultGroupHeap(targetSize, m.intGroupReverseLessThanFn)
	}
	for k, v := range m.intResults {
		group := intResultGroup{key: k, value: v}
		if m.intHeap.Len() < targetSize {
			m.intHeap.Push(group)
			continue
		}
		if min := m.intHeap.Min(); !m.intGroupReverseLessThanFn(min, group) {
			continue
		}
		m.intHeap.Pop()
		m.intHeap.Push(group)
	}

	// Allocate a new map and insert the intHeap into the map.
	m.intResults = make(map[int]calculation.ResultArray, targetSize)
	data := m.intHeap.RawData()
	for i := 0; i < len(data); i++ {
		m.intResults[data[i].key] = data[i].value
		data[i] = emptyIntResultGroup
	}
	m.intHeap.Reset()
}

func (m *SingleKeyResultGroups) trimDoubleToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	if m.doubleHeap == nil || m.doubleHeap.Cap() < targetSize {
		m.doubleHeap = newDoubleResultGroupHeap(targetSize, m.doubleGroupReverseLessThanFn)
	}
	for k, v := range m.doubleResults {
		group := doubleResultGroup{key: k, value: v}
		if m.doubleHeap.Len() < targetSize {
			m.doubleHeap.Push(group)
			continue
		}
		if min := m.doubleHeap.Min(); !m.doubleGroupReverseLessThanFn(min, group) {
			continue
		}
		m.doubleHeap.Pop()
		m.doubleHeap.Push(group)
	}

	// Allocate a new map and insert the doubleHeap doubleo the map.
	m.doubleResults = make(map[float64]calculation.ResultArray, targetSize)
	data := m.doubleHeap.RawData()
	for i := 0; i < len(data); i++ {
		m.doubleResults[data[i].key] = data[i].value
		data[i] = emptyDoubleResultGroup
	}
	m.doubleHeap.Reset()
}

func (m *SingleKeyResultGroups) trimStringToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	if m.stringHeap == nil || m.stringHeap.Cap() < targetSize {
		m.stringHeap = newStringResultGroupHeap(targetSize, m.stringGroupReverseLessThanFn)
	}
	for k, v := range m.stringResults {
		group := stringResultGroup{key: k, value: v}
		if m.stringHeap.Len() < targetSize {
			m.stringHeap.Push(group)
			continue
		}
		if min := m.stringHeap.Min(); !m.stringGroupReverseLessThanFn(min, group) {
			continue
		}
		m.stringHeap.Pop()
		m.stringHeap.Push(group)
	}

	// Allocate a new map and insert the stringHeap into the map.
	m.stringResults = make(map[string]calculation.ResultArray, targetSize)
	data := m.stringHeap.RawData()
	for i := 0; i < len(data); i++ {
		m.stringResults[data[i].key] = data[i].value
		data[i] = emptyStringResultGroup
	}
	m.stringHeap.Reset()
}

func (m *SingleKeyResultGroups) trimTimeToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	if m.timeHeap == nil || m.timeHeap.Cap() < targetSize {
		m.timeHeap = newTimeResultGroupHeap(targetSize, m.timeGroupReverseLessThanFn)
	}
	for k, v := range m.timeResults {
		group := timeResultGroup{key: k, value: v}
		if m.timeHeap.Len() < targetSize {
			m.timeHeap.Push(group)
			continue
		}
		if min := m.timeHeap.Min(); !m.timeGroupReverseLessThanFn(min, group) {
			continue
		}
		m.timeHeap.Pop()
		m.timeHeap.Push(group)
	}

	// Allocate a new map and insert the timeHeap timeo the map.
	m.timeResults = make(map[int64]calculation.ResultArray, targetSize)
	data := m.timeHeap.RawData()
	for i := 0; i < len(data); i++ {
		m.timeResults[data[i].key] = data[i].value
		data[i] = emptyTimeResultGroup
	}
	m.timeHeap.Reset()
}
