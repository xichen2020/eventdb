package query

import (
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
)

// InsertionStatus represents an insertion status.
type InsertionStatus int

// ForEachSingleKeyResultGroupFn is applied against each result group when iterating over
// result groups.
type ForEachSingleKeyResultGroupFn func(k field.ValueUnion, v calculation.ResultArray) bool

// A list of supported insertion status.
const (
	Existent InsertionStatus = iota
	Inserted
	RejectedDueToLimit
)

type lenFn func() int
type getOrInsertSingleKeyFn func(k *field.ValueUnion) (calculation.ResultArray, InsertionStatus)
type forEachSingleKeyGroupFn func(fn ForEachSingleKeyResultGroupFn)

// SingleKeyResultGroups stores the result mappings keyed on values from a single field
// whose values are of the same type.
type SingleKeyResultGroups struct {
	keyType              field.ValueType
	resultArrayProtoType calculation.ResultArray // Result array to create new result arrays from
	sizeLimit            int
	lenFn                lenFn
	getOrInsertFn        getOrInsertSingleKeyFn
	forEachGroupFn       forEachSingleKeyGroupFn

	nullResults   calculation.ResultArray
	boolResults   map[bool]calculation.ResultArray
	intResults    map[int]calculation.ResultArray
	doubleResults map[float64]calculation.ResultArray
	stringResults map[string]calculation.ResultArray
	timeResults   map[int64]calculation.ResultArray
}

// NewSingleKeyResultGroups creates a new single key result groups.
func NewSingleKeyResultGroups(
	keyType field.ValueType,
	resultArrayProtoType calculation.ResultArray,
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
	case field.BoolType:
		m.boolResults = make(map[bool]calculation.ResultArray, initCapacity)
		m.lenFn = m.getBoolLen
		m.getOrInsertFn = m.getOrInsertBool
		m.forEachGroupFn = m.forEachBoolGroup
	case field.IntType:
		m.intResults = make(map[int]calculation.ResultArray, initCapacity)
		m.lenFn = m.getIntLen
		m.getOrInsertFn = m.getOrInsertInt
		m.forEachGroupFn = m.forEachIntGroup
	case field.DoubleType:
		m.doubleResults = make(map[float64]calculation.ResultArray, initCapacity)
		m.lenFn = m.getDoubleLen
		m.getOrInsertFn = m.getOrInsertDouble
		m.forEachGroupFn = m.forEachDoubleGroup
	case field.StringType:
		m.stringResults = make(map[string]calculation.ResultArray, initCapacity)
		m.lenFn = m.getStringLen
		m.getOrInsertFn = m.getOrInsertString
		m.forEachGroupFn = m.forEachStringGroup
	case field.TimeType:
		m.timeResults = make(map[int64]calculation.ResultArray, initCapacity)
		m.lenFn = m.getTimeLen
		m.getOrInsertFn = m.getOrInsertTime
		m.forEachGroupFn = m.forEachTimeGroup
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

// MultiKeyResultGroups stores the result mappings keyed on an array of values
// from multiple fields.
type MultiKeyResultGroups struct {
	resultArrayProtoType calculation.ResultArray // Result array to create new result arrays from
	sizeLimit            int
	results              *ValuesResultArrayHash
}

// NewMultiKeyResultGroups creates a new multi key result groups object.
func NewMultiKeyResultGroups(
	resultArrayProtoType calculation.ResultArray,
	sizeLimit int,
	initCapacity int,
) *MultiKeyResultGroups {
	return &MultiKeyResultGroups{
		resultArrayProtoType: resultArrayProtoType,
		sizeLimit:            sizeLimit,
		results:              NewValuesResultArrayMap(initCapacity),
	}
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
