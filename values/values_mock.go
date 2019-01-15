// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/xichen2020/eventdb/values (interfaces: BoolValues,IntValues,DoubleValues,StringValues,TimeValues)

// Package values is a generated GoMock package.
package values

import (
	"reflect"

	"github.com/golang/mock/gomock"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/values/iterator"
)

// MockBoolValues is a mock of BoolValues interface
type MockBoolValues struct {
	ctrl     *gomock.Controller
	recorder *MockBoolValuesMockRecorder
}

// MockBoolValuesMockRecorder is the mock recorder for MockBoolValues
type MockBoolValuesMockRecorder struct {
	mock *MockBoolValues
}

// NewMockBoolValues creates a new mock instance
func NewMockBoolValues(ctrl *gomock.Controller) *MockBoolValues {
	mock := &MockBoolValues{ctrl: ctrl}
	mock.recorder = &MockBoolValuesMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockBoolValues) EXPECT() *MockBoolValuesMockRecorder {
	return m.recorder
}

// Filter mocks base method
func (m *MockBoolValues) Filter(arg0 filter.Op, arg1 *field.ValueUnion) (iterator.PositionIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Filter", arg0, arg1)
	ret0, _ := ret[0].(iterator.PositionIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Filter indicates an expected call of Filter
func (mr *MockBoolValuesMockRecorder) Filter(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Filter", reflect.TypeOf((*MockBoolValues)(nil).Filter), arg0, arg1)
}

// Iter mocks base method
func (m *MockBoolValues) Iter() (iterator.ForwardBoolIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iter")
	ret0, _ := ret[0].(iterator.ForwardBoolIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iter indicates an expected call of Iter
func (mr *MockBoolValuesMockRecorder) Iter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iter", reflect.TypeOf((*MockBoolValues)(nil).Iter))
}

// Metadata mocks base method
func (m *MockBoolValues) Metadata() BoolValuesMetadata {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Metadata")
	ret0, _ := ret[0].(BoolValuesMetadata)
	return ret0
}

// Metadata indicates an expected call of Metadata
func (mr *MockBoolValuesMockRecorder) Metadata() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Metadata", reflect.TypeOf((*MockBoolValues)(nil).Metadata))
}

// MockIntValues is a mock of IntValues interface
type MockIntValues struct {
	ctrl     *gomock.Controller
	recorder *MockIntValuesMockRecorder
}

// MockIntValuesMockRecorder is the mock recorder for MockIntValues
type MockIntValuesMockRecorder struct {
	mock *MockIntValues
}

// NewMockIntValues creates a new mock instance
func NewMockIntValues(ctrl *gomock.Controller) *MockIntValues {
	mock := &MockIntValues{ctrl: ctrl}
	mock.recorder = &MockIntValuesMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockIntValues) EXPECT() *MockIntValuesMockRecorder {
	return m.recorder
}

// Filter mocks base method
func (m *MockIntValues) Filter(arg0 filter.Op, arg1 *field.ValueUnion) (iterator.PositionIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Filter", arg0, arg1)
	ret0, _ := ret[0].(iterator.PositionIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Filter indicates an expected call of Filter
func (mr *MockIntValuesMockRecorder) Filter(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Filter", reflect.TypeOf((*MockIntValues)(nil).Filter), arg0, arg1)
}

// Iter mocks base method
func (m *MockIntValues) Iter() (iterator.ForwardIntIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iter")
	ret0, _ := ret[0].(iterator.ForwardIntIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iter indicates an expected call of Iter
func (mr *MockIntValuesMockRecorder) Iter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iter", reflect.TypeOf((*MockIntValues)(nil).Iter))
}

// Metadata mocks base method
func (m *MockIntValues) Metadata() IntValuesMetadata {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Metadata")
	ret0, _ := ret[0].(IntValuesMetadata)
	return ret0
}

// Metadata indicates an expected call of Metadata
func (mr *MockIntValuesMockRecorder) Metadata() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Metadata", reflect.TypeOf((*MockIntValues)(nil).Metadata))
}

// MockDoubleValues is a mock of DoubleValues interface
type MockDoubleValues struct {
	ctrl     *gomock.Controller
	recorder *MockDoubleValuesMockRecorder
}

// MockDoubleValuesMockRecorder is the mock recorder for MockDoubleValues
type MockDoubleValuesMockRecorder struct {
	mock *MockDoubleValues
}

// NewMockDoubleValues creates a new mock instance
func NewMockDoubleValues(ctrl *gomock.Controller) *MockDoubleValues {
	mock := &MockDoubleValues{ctrl: ctrl}
	mock.recorder = &MockDoubleValuesMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockDoubleValues) EXPECT() *MockDoubleValuesMockRecorder {
	return m.recorder
}

// Filter mocks base method
func (m *MockDoubleValues) Filter(arg0 filter.Op, arg1 *field.ValueUnion) (iterator.PositionIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Filter", arg0, arg1)
	ret0, _ := ret[0].(iterator.PositionIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Filter indicates an expected call of Filter
func (mr *MockDoubleValuesMockRecorder) Filter(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Filter", reflect.TypeOf((*MockDoubleValues)(nil).Filter), arg0, arg1)
}

// Iter mocks base method
func (m *MockDoubleValues) Iter() (iterator.ForwardDoubleIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iter")
	ret0, _ := ret[0].(iterator.ForwardDoubleIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iter indicates an expected call of Iter
func (mr *MockDoubleValuesMockRecorder) Iter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iter", reflect.TypeOf((*MockDoubleValues)(nil).Iter))
}

// Metadata mocks base method
func (m *MockDoubleValues) Metadata() DoubleValuesMetadata {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Metadata")
	ret0, _ := ret[0].(DoubleValuesMetadata)
	return ret0
}

// Metadata indicates an expected call of Metadata
func (mr *MockDoubleValuesMockRecorder) Metadata() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Metadata", reflect.TypeOf((*MockDoubleValues)(nil).Metadata))
}

// MockStringValues is a mock of StringValues interface
type MockStringValues struct {
	ctrl     *gomock.Controller
	recorder *MockStringValuesMockRecorder
}

// MockStringValuesMockRecorder is the mock recorder for MockStringValues
type MockStringValuesMockRecorder struct {
	mock *MockStringValues
}

// NewMockStringValues creates a new mock instance
func NewMockStringValues(ctrl *gomock.Controller) *MockStringValues {
	mock := &MockStringValues{ctrl: ctrl}
	mock.recorder = &MockStringValuesMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockStringValues) EXPECT() *MockStringValuesMockRecorder {
	return m.recorder
}

// Filter mocks base method
func (m *MockStringValues) Filter(arg0 filter.Op, arg1 *field.ValueUnion) (iterator.PositionIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Filter", arg0, arg1)
	ret0, _ := ret[0].(iterator.PositionIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Filter indicates an expected call of Filter
func (mr *MockStringValuesMockRecorder) Filter(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Filter", reflect.TypeOf((*MockStringValues)(nil).Filter), arg0, arg1)
}

// Iter mocks base method
func (m *MockStringValues) Iter() (iterator.ForwardStringIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iter")
	ret0, _ := ret[0].(iterator.ForwardStringIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iter indicates an expected call of Iter
func (mr *MockStringValuesMockRecorder) Iter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iter", reflect.TypeOf((*MockStringValues)(nil).Iter))
}

// Metadata mocks base method
func (m *MockStringValues) Metadata() StringValuesMetadata {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Metadata")
	ret0, _ := ret[0].(StringValuesMetadata)
	return ret0
}

// Metadata indicates an expected call of Metadata
func (mr *MockStringValuesMockRecorder) Metadata() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Metadata", reflect.TypeOf((*MockStringValues)(nil).Metadata))
}

// MockTimeValues is a mock of TimeValues interface
type MockTimeValues struct {
	ctrl     *gomock.Controller
	recorder *MockTimeValuesMockRecorder
}

// MockTimeValuesMockRecorder is the mock recorder for MockTimeValues
type MockTimeValuesMockRecorder struct {
	mock *MockTimeValues
}

// NewMockTimeValues creates a new mock instance
func NewMockTimeValues(ctrl *gomock.Controller) *MockTimeValues {
	mock := &MockTimeValues{ctrl: ctrl}
	mock.recorder = &MockTimeValuesMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockTimeValues) EXPECT() *MockTimeValuesMockRecorder {
	return m.recorder
}

// Filter mocks base method
func (m *MockTimeValues) Filter(arg0 filter.Op, arg1 *field.ValueUnion) (iterator.PositionIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Filter", arg0, arg1)
	ret0, _ := ret[0].(iterator.PositionIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Filter indicates an expected call of Filter
func (mr *MockTimeValuesMockRecorder) Filter(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Filter", reflect.TypeOf((*MockTimeValues)(nil).Filter), arg0, arg1)
}

// Iter mocks base method
func (m *MockTimeValues) Iter() (iterator.ForwardTimeIterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iter")
	ret0, _ := ret[0].(iterator.ForwardTimeIterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iter indicates an expected call of Iter
func (mr *MockTimeValuesMockRecorder) Iter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iter", reflect.TypeOf((*MockTimeValues)(nil).Iter))
}

// Metadata mocks base method
func (m *MockTimeValues) Metadata() TimeValuesMetadata {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Metadata")
	ret0, _ := ret[0].(TimeValuesMetadata)
	return ret0
}

// Metadata indicates an expected call of Metadata
func (mr *MockTimeValuesMockRecorder) Metadata() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Metadata", reflect.TypeOf((*MockTimeValues)(nil).Metadata))
}