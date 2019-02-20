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
	closed        bool
	cnt           *refcnt.RefCounter
	p             *BucketizedBoolArrayPool
	vals          []bool
	valuesResetFn *func(values []bool)
}

// NewRefCountedPooledBoolArray creates a new refcounted, pooled generic value array.
func NewRefCountedPooledBoolArray(
	vals []bool,
	p *BucketizedBoolArrayPool,
	resetFn *func(values []bool),
) *RefCountedPooledBoolArray {
	return &RefCountedPooledBoolArray{
		cnt:           refcnt.NewRefCounter(),
		p:             p,
		vals:          vals,
		valuesResetFn: resetFn,
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
	if rv.valuesResetFn != nil {
		(*rv.valuesResetFn)(rv.vals)
	}
	rv.vals = rv.vals[:0]
	rv.p.Put(rv.vals, cap(rv.vals))
	rv.vals = nil
	rv.cnt = nil
}
