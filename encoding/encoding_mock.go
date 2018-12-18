// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/xichen2020/eventdb/encoding (interfaces: RewindableIntIterator,RewindableStringIterator,ForwardDoubleIterator,ForwardBoolIterator)

// Package encoding is a generated GoMock package.
package encoding

import (
	"reflect"

	"github.com/golang/mock/gomock"
)

// MockRewindableIntIterator is a mock of RewindableIntIterator interface
type MockRewindableIntIterator struct {
	ctrl     *gomock.Controller
	recorder *MockRewindableIntIteratorMockRecorder
}

// MockRewindableIntIteratorMockRecorder is the mock recorder for MockRewindableIntIterator
type MockRewindableIntIteratorMockRecorder struct {
	mock *MockRewindableIntIterator
}

// NewMockRewindableIntIterator creates a new mock instance
func NewMockRewindableIntIterator(ctrl *gomock.Controller) *MockRewindableIntIterator {
	mock := &MockRewindableIntIterator{ctrl: ctrl}
	mock.recorder = &MockRewindableIntIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockRewindableIntIterator) EXPECT() *MockRewindableIntIteratorMockRecorder {
	return m.recorder
}

// Close mocks base method
func (m *MockRewindableIntIterator) Close() error {
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (mr *MockRewindableIntIteratorMockRecorder) Close() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRewindableIntIterator)(nil).Close))
}

// Current mocks base method
func (m *MockRewindableIntIterator) Current() int {
	ret := m.ctrl.Call(m, "Current")
	ret0, _ := ret[0].(int)
	return ret0
}

// Current indicates an expected call of Current
func (mr *MockRewindableIntIteratorMockRecorder) Current() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Current", reflect.TypeOf((*MockRewindableIntIterator)(nil).Current))
}

// Err mocks base method
func (m *MockRewindableIntIterator) Err() error {
	ret := m.ctrl.Call(m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

// Err indicates an expected call of Err
func (mr *MockRewindableIntIteratorMockRecorder) Err() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Err", reflect.TypeOf((*MockRewindableIntIterator)(nil).Err))
}

// Next mocks base method
func (m *MockRewindableIntIterator) Next() bool {
	ret := m.ctrl.Call(m, "Next")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Next indicates an expected call of Next
func (mr *MockRewindableIntIteratorMockRecorder) Next() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockRewindableIntIterator)(nil).Next))
}

// Reset mocks base method
func (m *MockRewindableIntIterator) Reset(arg0 []int) {
	m.ctrl.Call(m, "Reset", arg0)
}

// Reset indicates an expected call of Reset
func (mr *MockRewindableIntIteratorMockRecorder) Reset(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reset", reflect.TypeOf((*MockRewindableIntIterator)(nil).Reset), arg0)
}

// Rewind mocks base method
func (m *MockRewindableIntIterator) Rewind() {
	m.ctrl.Call(m, "Rewind")
}

// Rewind indicates an expected call of Rewind
func (mr *MockRewindableIntIteratorMockRecorder) Rewind() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rewind", reflect.TypeOf((*MockRewindableIntIterator)(nil).Rewind))
}

// MockRewindableStringIterator is a mock of RewindableStringIterator interface
type MockRewindableStringIterator struct {
	ctrl     *gomock.Controller
	recorder *MockRewindableStringIteratorMockRecorder
}

// MockRewindableStringIteratorMockRecorder is the mock recorder for MockRewindableStringIterator
type MockRewindableStringIteratorMockRecorder struct {
	mock *MockRewindableStringIterator
}

// NewMockRewindableStringIterator creates a new mock instance
func NewMockRewindableStringIterator(ctrl *gomock.Controller) *MockRewindableStringIterator {
	mock := &MockRewindableStringIterator{ctrl: ctrl}
	mock.recorder = &MockRewindableStringIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockRewindableStringIterator) EXPECT() *MockRewindableStringIteratorMockRecorder {
	return m.recorder
}

// Close mocks base method
func (m *MockRewindableStringIterator) Close() error {
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (mr *MockRewindableStringIteratorMockRecorder) Close() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRewindableStringIterator)(nil).Close))
}

// Current mocks base method
func (m *MockRewindableStringIterator) Current() string {
	ret := m.ctrl.Call(m, "Current")
	ret0, _ := ret[0].(string)
	return ret0
}

// Current indicates an expected call of Current
func (mr *MockRewindableStringIteratorMockRecorder) Current() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Current", reflect.TypeOf((*MockRewindableStringIterator)(nil).Current))
}

// Err mocks base method
func (m *MockRewindableStringIterator) Err() error {
	ret := m.ctrl.Call(m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

// Err indicates an expected call of Err
func (mr *MockRewindableStringIteratorMockRecorder) Err() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Err", reflect.TypeOf((*MockRewindableStringIterator)(nil).Err))
}

// Next mocks base method
func (m *MockRewindableStringIterator) Next() bool {
	ret := m.ctrl.Call(m, "Next")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Next indicates an expected call of Next
func (mr *MockRewindableStringIteratorMockRecorder) Next() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockRewindableStringIterator)(nil).Next))
}

// Reset mocks base method
func (m *MockRewindableStringIterator) Reset(arg0 []string) {
	m.ctrl.Call(m, "Reset", arg0)
}

// Reset indicates an expected call of Reset
func (mr *MockRewindableStringIteratorMockRecorder) Reset(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reset", reflect.TypeOf((*MockRewindableStringIterator)(nil).Reset), arg0)
}

// Rewind mocks base method
func (m *MockRewindableStringIterator) Rewind() {
	m.ctrl.Call(m, "Rewind")
}

// Rewind indicates an expected call of Rewind
func (mr *MockRewindableStringIteratorMockRecorder) Rewind() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rewind", reflect.TypeOf((*MockRewindableStringIterator)(nil).Rewind))
}

// MockForwardDoubleIterator is a mock of ForwardDoubleIterator interface
type MockForwardDoubleIterator struct {
	ctrl     *gomock.Controller
	recorder *MockForwardDoubleIteratorMockRecorder
}

// MockForwardDoubleIteratorMockRecorder is the mock recorder for MockForwardDoubleIterator
type MockForwardDoubleIteratorMockRecorder struct {
	mock *MockForwardDoubleIterator
}

// NewMockForwardDoubleIterator creates a new mock instance
func NewMockForwardDoubleIterator(ctrl *gomock.Controller) *MockForwardDoubleIterator {
	mock := &MockForwardDoubleIterator{ctrl: ctrl}
	mock.recorder = &MockForwardDoubleIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockForwardDoubleIterator) EXPECT() *MockForwardDoubleIteratorMockRecorder {
	return m.recorder
}

// Close mocks base method
func (m *MockForwardDoubleIterator) Close() error {
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (mr *MockForwardDoubleIteratorMockRecorder) Close() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockForwardDoubleIterator)(nil).Close))
}

// Current mocks base method
func (m *MockForwardDoubleIterator) Current() float64 {
	ret := m.ctrl.Call(m, "Current")
	ret0, _ := ret[0].(float64)
	return ret0
}

// Current indicates an expected call of Current
func (mr *MockForwardDoubleIteratorMockRecorder) Current() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Current", reflect.TypeOf((*MockForwardDoubleIterator)(nil).Current))
}

// Err mocks base method
func (m *MockForwardDoubleIterator) Err() error {
	ret := m.ctrl.Call(m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

// Err indicates an expected call of Err
func (mr *MockForwardDoubleIteratorMockRecorder) Err() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Err", reflect.TypeOf((*MockForwardDoubleIterator)(nil).Err))
}

// Next mocks base method
func (m *MockForwardDoubleIterator) Next() bool {
	ret := m.ctrl.Call(m, "Next")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Next indicates an expected call of Next
func (mr *MockForwardDoubleIteratorMockRecorder) Next() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockForwardDoubleIterator)(nil).Next))
}

// MockForwardBoolIterator is a mock of ForwardBoolIterator interface
type MockForwardBoolIterator struct {
	ctrl     *gomock.Controller
	recorder *MockForwardBoolIteratorMockRecorder
}

// MockForwardBoolIteratorMockRecorder is the mock recorder for MockForwardBoolIterator
type MockForwardBoolIteratorMockRecorder struct {
	mock *MockForwardBoolIterator
}

// NewMockForwardBoolIterator creates a new mock instance
func NewMockForwardBoolIterator(ctrl *gomock.Controller) *MockForwardBoolIterator {
	mock := &MockForwardBoolIterator{ctrl: ctrl}
	mock.recorder = &MockForwardBoolIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockForwardBoolIterator) EXPECT() *MockForwardBoolIteratorMockRecorder {
	return m.recorder
}

// Close mocks base method
func (m *MockForwardBoolIterator) Close() error {
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close
func (mr *MockForwardBoolIteratorMockRecorder) Close() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockForwardBoolIterator)(nil).Close))
}

// Current mocks base method
func (m *MockForwardBoolIterator) Current() bool {
	ret := m.ctrl.Call(m, "Current")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Current indicates an expected call of Current
func (mr *MockForwardBoolIteratorMockRecorder) Current() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Current", reflect.TypeOf((*MockForwardBoolIterator)(nil).Current))
}

// Err mocks base method
func (m *MockForwardBoolIterator) Err() error {
	ret := m.ctrl.Call(m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

// Err indicates an expected call of Err
func (mr *MockForwardBoolIteratorMockRecorder) Err() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Err", reflect.TypeOf((*MockForwardBoolIterator)(nil).Err))
}

// Next mocks base method
func (m *MockForwardBoolIterator) Next() bool {
	ret := m.ctrl.Call(m, "Next")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Next indicates an expected call of Next
func (mr *MockForwardBoolIteratorMockRecorder) Next() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockForwardBoolIterator)(nil).Next))
}
