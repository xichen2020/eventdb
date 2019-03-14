package template

import (
	"github.com/xichen2020/eventdb/x/refcnt"

	"github.com/mauricelam/genny/generic"
)

// GenericBucketizedValueArrayPool is a generic bucketized value array pool.
type GenericBucketizedValueArrayPool interface {
	generic.Type

	Get(capacity int) []GenericValue
	Put(values []GenericValue, capacity int)
}

// RefCountedPooledValueArray is a refcounted, pooled generic value array.
type RefCountedPooledValueArray struct {
	closed        bool
	cnt           *refcnt.RefCounter
	p             GenericBucketizedValueArrayPool
	vals          []GenericValue
	valuesResetFn func(values []GenericValue)
}

// NewRefCountedPooledValueArray creates a new refcounted, pooled generic value array.
func NewRefCountedPooledValueArray(
	vals []GenericValue,
	p GenericBucketizedValueArrayPool,
	resetFn func(values []GenericValue),
) *RefCountedPooledValueArray {
	return &RefCountedPooledValueArray{
		cnt:           refcnt.NewRefCounter(),
		p:             p,
		vals:          vals,
		valuesResetFn: resetFn,
	}
}

// Get returns the underlying raw value array.
func (rv *RefCountedPooledValueArray) Get() []GenericValue { return rv.vals }

// Snapshot takes a snapshot of the current values in the refcounted array.
// The returned snapshot shares the backing array with the source array but
// keeps a copy of the array slice as the snapshot. As a result, new values
// appended to the end of the array after the snapshot is taken is invisible
// to the snapshot.
func (rv *RefCountedPooledValueArray) Snapshot() *RefCountedPooledValueArray {
	rv.cnt.IncRef()
	return &RefCountedPooledValueArray{
		cnt:  rv.cnt,
		p:    rv.p,
		vals: rv.vals,
	}
}

// Append appends a value to the value array.
func (rv *RefCountedPooledValueArray) Append(v GenericValue) {
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
func (rv *RefCountedPooledValueArray) Close() {
	if rv.closed {
		return
	}
	rv.closed = true
	rv.tryRelease()
}

func (rv *RefCountedPooledValueArray) tryRelease() {
	if rv.cnt.DecRef() > 0 {
		return
	}
	if rv.valuesResetFn != nil {
		rv.valuesResetFn(rv.vals)
	}
	rv.vals = rv.vals[:0]
	rv.p.Put(rv.vals, cap(rv.vals))
	rv.vals = nil
	rv.cnt = nil
}
