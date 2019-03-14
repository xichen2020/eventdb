package query

import (
	"encoding/json"
	"fmt"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
)

// ForEachSingleKeyResultGroupFn is applied against each result group when iterating over
// result groups.
type ForEachSingleKeyResultGroupFn func(k field.ValueUnion, v calculation.ResultArray) bool

type lenFn func() int
type getOrInsertSingleKeyFn func(k *field.ValueUnion) (calculation.ResultArray, InsertionStatus)
type mergeSingleKeyGroupInPlaceFn func(other *SingleKeyResultGroups)
type marshalJSONFn func(numGroups int, topNRequired bool) ([]byte, error)
type toProtoFn func(numGroups int, topNRequired bool) *servicepb.SingleKeyGroupQueryResults
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
	toProtoFn                    toProtoFn
	trimToTopNFn                 trimToTopNFn
	boolGroupReverseLessThanFn   boolResultGroupLessThanFn
	intGroupReverseLessThanFn    intResultGroupLessThanFn
	doubleGroupReverseLessThanFn doubleResultGroupLessThanFn
	stringGroupReverseLessThanFn bytesResultGroupLessThanFn
	timeGroupReverseLessThanFn   timeResultGroupLessThanFn

	nullResults   calculation.ResultArray
	boolResults   map[bool]calculation.ResultArray
	intResults    map[int]calculation.ResultArray
	doubleResults map[float64]calculation.ResultArray
	bytesResults  *BytesResultArrayHashMap
	timeResults   map[int64]calculation.ResultArray

	topNBools   *topNBools
	topNInts    *topNInts
	topNDoubles *topNDoubles
	topNBytes   *topNBytes
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
		m.toProtoFn = m.toProtoNullGroups
		m.trimToTopNFn = m.trimNullToTopN
	case field.BoolType:
		m.boolResults = make(map[bool]calculation.ResultArray, initCapacity)
		m.lenFn = m.getBoolLen
		m.getOrInsertFn = m.getOrInsertBool
		m.mergeInPlaceFn = m.mergeBoolGroups
		m.marshalJSONFn = m.marshalJSONBoolGroups
		m.toProtoFn = m.toProtoBoolGroups
		m.trimToTopNFn = m.trimBoolToTopN
		m.boolGroupReverseLessThanFn, err = newBoolResultGroupReverseLessThanFn(orderBy)
	case field.IntType:
		m.intResults = make(map[int]calculation.ResultArray, initCapacity)
		m.lenFn = m.getIntLen
		m.getOrInsertFn = m.getOrInsertInt
		m.mergeInPlaceFn = m.mergeIntGroups
		m.marshalJSONFn = m.marshalJSONIntGroups
		m.toProtoFn = m.toProtoIntGroups
		m.trimToTopNFn = m.trimIntToTopN
		m.intGroupReverseLessThanFn, err = newIntResultGroupReverseLessThanFn(orderBy)
	case field.DoubleType:
		m.doubleResults = make(map[float64]calculation.ResultArray, initCapacity)
		m.lenFn = m.getDoubleLen
		m.getOrInsertFn = m.getOrInsertDouble
		m.mergeInPlaceFn = m.mergeDoubleGroups
		m.marshalJSONFn = m.marshalJSONDoubleGroups
		m.toProtoFn = m.toProtoDoubleGroups
		m.trimToTopNFn = m.trimDoubleToTopN
		m.doubleGroupReverseLessThanFn, err = newDoubleResultGroupReverseLessThanFn(orderBy)
	case field.BytesType:
		m.bytesResults = NewBytesResultArrayHashMap(BytesResultArrayHashMapOptions{
			InitialSize: initCapacity,
		})
		m.lenFn = m.getBytesLen
		m.getOrInsertFn = m.getOrInsertBytes
		m.mergeInPlaceFn = m.mergeBytesGroups
		m.marshalJSONFn = m.marshalJSONBytesGroups
		m.toProtoFn = m.toProtoBytesGroups
		m.trimToTopNFn = m.trimBytesToTopN
		m.stringGroupReverseLessThanFn, err = newBytesResultGroupReverseLessThanFn(orderBy)
	case field.TimeType:
		m.timeResults = make(map[int64]calculation.ResultArray, initCapacity)
		m.lenFn = m.getTimeLen
		m.getOrInsertFn = m.getOrInsertTime
		m.mergeInPlaceFn = m.mergeTimeGroups
		m.marshalJSONFn = m.marshalJSONTimeGroups
		m.toProtoFn = m.toProtoTimeGroups
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

// ToProto converts the single key result groups to single key result groups proto message.
func (m *SingleKeyResultGroups) ToProto(
	numGroups int,
	topNRequired bool,
) *servicepb.SingleKeyGroupQueryResults {
	return m.toProtoFn(numGroups, topNRequired)
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
	m.bytesResults = nil
	m.timeResults = nil
	m.topNBools = nil
	m.topNInts = nil
	m.topNDoubles = nil
	m.topNBytes = nil
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
func (m *SingleKeyResultGroups) getBytesLen() int  { return m.bytesResults.Len() }
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

func (m *SingleKeyResultGroups) getOrInsertBytes(
	key *field.ValueUnion,
) (calculation.ResultArray, InsertionStatus) {
	v := key.BytesVal
	arr, exists := m.bytesResults.Get(v.Bytes())
	if exists {
		return arr, Existent
	}
	if m.bytesResults.Len() >= m.sizeLimit {
		return nil, RejectedDueToLimit
	}
	arr = m.resultArrayProtoType.New()
	m.bytesResults.SetUnsafe(v.SafeBytes(), arr, SetUnsafeBytesOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
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

func (m *SingleKeyResultGroups) mergeBytesGroups(other *SingleKeyResultGroups) {
	if other.bytesResults.Len() == 0 {
		return
	}
	if m.bytesResults.Len() == 0 {
		m.bytesResults = other.bytesResults
		return
	}
	for _, entry := range other.bytesResults.Iter() {
		k, v := entry.Key(), entry.Value()
		currVal, exists := m.bytesResults.Get(k)
		if exists {
			currVal.MergeInPlace(v)
			continue
		}
		// About to insert a new group.
		if m.bytesResults.Len() >= m.sizeLimit {
			// Limit reached, do nothing.
			continue
		}
		m.bytesResults.SetUnsafe(k, v, SetUnsafeBytesOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		})
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

func (m *SingleKeyResultGroups) marshalJSONNullGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	res := m.computeNullGroups(numGroups, topNRequired)
	groups := nullResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONBoolGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	res := m.computeBoolGroups(numGroups, topNRequired)
	groups := boolResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONIntGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	res := m.computeIntGroups(numGroups, topNRequired)
	groups := intResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONDoubleGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	res := m.computeDoubleGroups(numGroups, topNRequired)
	groups := doubleResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONBytesGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	res := m.computeBytesGroups(numGroups, topNRequired)
	groups := bytesResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) marshalJSONTimeGroups(
	numGroups int,
	topNRequired bool,
) ([]byte, error) {
	res := m.computeTimeGroups(numGroups, topNRequired)
	groups := timeResultGroupsJSON{Groups: res}
	return json.Marshal(groups)
}

func (m *SingleKeyResultGroups) toProtoNullGroups(
	numGroups int,
	topNRequired bool,
) *servicepb.SingleKeyGroupQueryResults {
	var (
		groups  = m.computeNullGroups(numGroups, topNRequired)
		results = make([]servicepb.SingleKeyGroupQueryResult, 0, len(groups))
	)
	for _, g := range groups {
		pbKey := servicepb.FieldValue{
			Type: servicepb.FieldValue_NULL,
		}
		pbValues := g.Values.ToProto()
		results = append(results, servicepb.SingleKeyGroupQueryResult{
			Key:    pbKey,
			Values: pbValues,
		})
	}
	return &servicepb.SingleKeyGroupQueryResults{
		Groups: results,
	}
}

func (m *SingleKeyResultGroups) toProtoBoolGroups(
	numGroups int,
	topNRequired bool,
) *servicepb.SingleKeyGroupQueryResults {
	var (
		groups  = m.computeBoolGroups(numGroups, topNRequired)
		results = make([]servicepb.SingleKeyGroupQueryResult, 0, len(groups))
	)
	for _, g := range groups {
		pbKey := servicepb.FieldValue{
			Type:    servicepb.FieldValue_BOOL,
			BoolVal: g.Key,
		}
		pbValues := g.Values.ToProto()
		results = append(results, servicepb.SingleKeyGroupQueryResult{
			Key:    pbKey,
			Values: pbValues,
		})
	}
	return &servicepb.SingleKeyGroupQueryResults{
		Groups: results,
	}
}

func (m *SingleKeyResultGroups) toProtoIntGroups(
	numGroups int,
	topNRequired bool,
) *servicepb.SingleKeyGroupQueryResults {
	var (
		groups  = m.computeIntGroups(numGroups, topNRequired)
		results = make([]servicepb.SingleKeyGroupQueryResult, 0, len(groups))
	)
	for _, g := range groups {
		pbKey := servicepb.FieldValue{
			Type:   servicepb.FieldValue_INT,
			IntVal: int64(g.Key),
		}
		pbValues := g.Values.ToProto()
		results = append(results, servicepb.SingleKeyGroupQueryResult{
			Key:    pbKey,
			Values: pbValues,
		})
	}
	return &servicepb.SingleKeyGroupQueryResults{
		Groups: results,
	}
}

func (m *SingleKeyResultGroups) toProtoDoubleGroups(
	numGroups int,
	topNRequired bool,
) *servicepb.SingleKeyGroupQueryResults {
	var (
		groups  = m.computeDoubleGroups(numGroups, topNRequired)
		results = make([]servicepb.SingleKeyGroupQueryResult, 0, len(groups))
	)
	for _, g := range groups {
		pbKey := servicepb.FieldValue{
			Type:      servicepb.FieldValue_DOUBLE,
			DoubleVal: g.Key,
		}
		pbValues := g.Values.ToProto()
		results = append(results, servicepb.SingleKeyGroupQueryResult{
			Key:    pbKey,
			Values: pbValues,
		})
	}
	return &servicepb.SingleKeyGroupQueryResults{
		Groups: results,
	}
}

func (m *SingleKeyResultGroups) toProtoBytesGroups(
	numGroups int,
	topNRequired bool,
) *servicepb.SingleKeyGroupQueryResults {
	var (
		groups  = m.computeBytesGroups(numGroups, topNRequired)
		results = make([]servicepb.SingleKeyGroupQueryResult, 0, len(groups))
	)
	for _, g := range groups {
		pbKey := servicepb.FieldValue{
			Type:     servicepb.FieldValue_BYTES,
			BytesVal: g.Key,
		}
		pbValues := g.Values.ToProto()
		results = append(results, servicepb.SingleKeyGroupQueryResult{
			Key:    pbKey,
			Values: pbValues,
		})
	}
	return &servicepb.SingleKeyGroupQueryResults{
		Groups: results,
	}
}

func (m *SingleKeyResultGroups) toProtoTimeGroups(
	numGroups int,
	topNRequired bool,
) *servicepb.SingleKeyGroupQueryResults {
	var (
		groups  = m.computeTimeGroups(numGroups, topNRequired)
		results = make([]servicepb.SingleKeyGroupQueryResult, 0, len(groups))
	)
	for _, g := range groups {
		pbKey := servicepb.FieldValue{
			Type:         servicepb.FieldValue_TIME,
			TimeNanosVal: g.Key,
		}
		pbValues := g.Values.ToProto()
		results = append(results, servicepb.SingleKeyGroupQueryResult{
			Key:    pbKey,
			Values: pbValues,
		})
	}
	return &servicepb.SingleKeyGroupQueryResults{
		Groups: results,
	}
}

func (m *SingleKeyResultGroups) computeNullGroups(numGroups int, _ bool) []nullResultGroup {
	if numGroups <= 0 {
		return nil
	}
	return []nullResultGroup{
		{Values: m.nullResults},
	}
}

func (m *SingleKeyResultGroups) computeBoolGroups(
	numGroups int,
	topNRequired bool,
) []boolResultGroup {
	if numGroups <= 0 {
		return nil
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
	return res
}

func (m *SingleKeyResultGroups) computeIntGroups(
	numGroups int,
	topNRequired bool,
) []intResultGroup {
	if numGroups <= 0 {
		return nil
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
	return res
}

func (m *SingleKeyResultGroups) computeDoubleGroups(
	numGroups int,
	topNRequired bool,
) []doubleResultGroup {
	if numGroups <= 0 {
		return nil
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
	return res
}

func (m *SingleKeyResultGroups) computeBytesGroups(
	numGroups int,
	topNRequired bool,
) []bytesResultGroup {
	if numGroups <= 0 {
		return nil
	}
	var res []bytesResultGroup
	if topNRequired {
		m.computeTopNBytesGroups(numGroups)
		res = m.topNBytes.SortInPlace()
	} else {
		res = make([]bytesResultGroup, 0, numGroups)
		for _, entry := range m.bytesResults.Iter() {
			group := bytesResultGroup{Key: entry.Key(), Values: entry.Value()}
			res = append(res, group)
		}
	}
	return res
}

func (m *SingleKeyResultGroups) computeTimeGroups(
	numGroups int,
	topNRequired bool,
) []timeResultGroup {
	if numGroups <= 0 {
		return nil
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
	return res
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

// computeTopNBytesGroups computes the top N string groups and stores them in `topNBytes`.
func (m *SingleKeyResultGroups) computeTopNBytesGroups(targetSize int) {
	if m.topNBytes == nil || m.topNBytes.Cap() < targetSize {
		m.topNBytes = newTopNBytes(targetSize, m.stringGroupReverseLessThanFn)
	}
	for _, entry := range m.bytesResults.Iter() {
		group := bytesResultGroup{Key: entry.Key(), Values: entry.Value()}
		m.topNBytes.Add(group, bytesAddOptions{})
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

func (m *SingleKeyResultGroups) trimBytesToTopN(targetSize int) {
	if m.Len() <= targetSize {
		return
	}

	// Find the top N groups.
	m.computeTopNBytesGroups(targetSize)

	// Allocate a new map and insert the top n strings into the map.
	m.bytesResults = NewBytesResultArrayHashMap(BytesResultArrayHashMapOptions{
		InitialSize: targetSize,
	})
	data := m.topNBytes.RawData()
	for i := 0; i < len(data); i++ {
		m.bytesResults.SetUnsafe(data[i].Key, data[i].Values, SetUnsafeBytesOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		})
		data[i] = emptyBytesResultGroup
	}
	m.topNBytes.Reset()
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
