package query

import (
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
)

const (
	defaultInitResultGroupCapacity = 4096
)

// InsertionStatus represents an insertion status.
type InsertionStatus int

// A list of supported insertion status.
const (
	Existent InsertionStatus = iota
	Inserted
	RejectedDueToLimit
)

type lenFn func() int
type getOrInsertSingleKeyFn func(k *field.ValueUnion) (calculation.ResultArray, InsertionStatus)

// SingleKeyResultGroups stores the result mappings keyed on values from a single field
// whose values are of the same type.
type SingleKeyResultGroups struct {
	keyType              field.ValueType
	resultArrayProtoType calculation.ResultArray // Result array to create new result arrays from
	sizeLimit            int
	lenFn                lenFn
	getOrInsertFn        getOrInsertSingleKeyFn

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
	case field.BoolType:
		m.boolResults = make(map[bool]calculation.ResultArray, defaultInitResultGroupCapacity)
		m.lenFn = m.getBoolLen
		m.getOrInsertFn = m.getOrInsertBool
	case field.IntType:
		m.intResults = make(map[int]calculation.ResultArray, defaultInitResultGroupCapacity)
		m.lenFn = m.getIntLen
		m.getOrInsertFn = m.getOrInsertInt
	case field.DoubleType:
		m.doubleResults = make(map[float64]calculation.ResultArray, defaultInitResultGroupCapacity)
		m.lenFn = m.getDoubleLen
		m.getOrInsertFn = m.getOrInsertDouble
	case field.StringType:
		m.stringResults = make(map[string]calculation.ResultArray, defaultInitResultGroupCapacity)
		m.lenFn = m.getStringLen
		m.getOrInsertFn = m.getOrInsertString
	case field.TimeType:
		m.timeResults = make(map[int64]calculation.ResultArray, defaultInitResultGroupCapacity)
		m.lenFn = m.getTimeLen
		m.getOrInsertFn = m.getOrInsertTime
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
