// Copyright (c) 2019 Uber Technologies, Inc.
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

// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package pool

import (
	"github.com/xichen2020/eventdb/x/refcnt"
)

// *BucketizedBoolArrayPool is a generic bucketized value array pool.

// RefCountedPooledBoolArray is a refcounted, pooled generic value array.
type RefCountedPooledBoolArray struct {
	closed bool
	cnt    *refcnt.RefCounter
	p      *BucketizedBoolArrayPool
	vals   []bool
}

// NewRefCountedPooledBoolArray creates a new refcounted, pooled generic value array.
func NewRefCountedPooledBoolArray(
	vals []bool,
	p *BucketizedBoolArrayPool,
) *RefCountedPooledBoolArray {
	return &RefCountedPooledBoolArray{
		cnt:  refcnt.NewRefCounter(),
		p:    p,
		vals: vals,
	}
}

// Get returns the underlying raw value array.
func (rv *RefCountedPooledBoolArray) Get() []bool { return rv.vals }

// Snapshot takes a snapshot of the current values in the refcounted array.
// The returned snapshot shares the backing array with the source array but
// keeps a copy of the array slice as the snapshot. As a result, new values
// appended to the end of the array after the snapshot is taken is invisible
// to the snapshot.
func (rv *RefCountedPooledBoolArray) Snapshot() *RefCountedPooledBoolArray {
	rv.cnt.IncRef()
	return &RefCountedPooledBoolArray{
		cnt:  rv.cnt,
		p:    rv.p,
		vals: rv.vals,
	}
}

// Append appends a value to the value array.
func (rv *RefCountedPooledBoolArray) Append(v bool) {
	if len(rv.vals) < cap(rv.vals) {
		rv.vals = append(rv.vals, v)
		return
	}
	newVals := rv.p.Get(cap(rv.vals) * 2)
	n := copy(newVals[:len(rv.vals)], rv.vals)
	newVals = newVals[:n]
	newVals = append(newVals, v)
	rv.tryRelease()
	rv.cnt = refcnt.NewRefCounter()
	rv.vals = newVals
}

// Close closes the ref counted array.
func (rv *RefCountedPooledBoolArray) Close() {
	if rv.closed {
		return
	}
	rv.closed = true
	rv.tryRelease()
}

func (rv *RefCountedPooledBoolArray) tryRelease() {
	if rv.cnt.DecRef() > 0 {
		return
	}
	rv.vals = rv.vals[:0]
	rv.p.Put(rv.vals, cap(rv.vals))
	rv.vals = nil
	rv.cnt = nil
}
