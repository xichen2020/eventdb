package refcnt

import (
	"fmt"
	"sync/atomic"
)

// RefCountable is an object that is reference counted.
type RefCountable interface {
	// IncRef increments the reference count.
	IncRef()

	// DecRef decrements the reference count.
	// When the reference count goes to zero,
	// an optional callback is executed.
	DecRef()
}

// OnZeroRefCountFn is a callback that gets called when the reference
// count of an object goes to zero.
type OnZeroRefCountFn func()

// RefCounter is a reference counter.
type RefCounter struct {
	n                int32
	onZeroRefCountFn OnZeroRefCountFn
}

// NewRefCounter creates a new reference counter, with an initial
// refcount of 1.
func NewRefCounter(fn OnZeroRefCountFn) *RefCounter {
	return &RefCounter{n: 1, onZeroRefCountFn: fn}
}

// IncRef increments the ref count.
func (c *RefCounter) IncRef() {
	n := int(atomic.AddInt32(&c.n, 1))
	if n > 0 {
		return
	}
	panic(fmt.Errorf("invalid ref count %d", n))
}

// DecRef decrements the ref count, and optionally executes the
// callback where applicable.
func (c *RefCounter) DecRef() {
	n := int(atomic.AddInt32(&c.n, -1))
	if n > 0 {
		return
	}
	if n == 0 {
		if c.onZeroRefCountFn != nil {
			c.onZeroRefCountFn()
		}
		return
	}
	panic(fmt.Errorf("invalid ref count %d", n))
}
