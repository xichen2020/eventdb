package refcnt

import (
	"fmt"
	"sync/atomic"
)

// RefCountable is an object that is reference counted.
type RefCountable interface {
	// IncRef increments the reference count.
	IncRef() int32

	// DecRef decrements the reference count.
	// When the reference count goes to zero,
	// an optional callback is executed.
	DecRef() int32

	// RefCount returns the current reference count.
	RefCount() int32
}

// RefCounter is a reference counter.
type RefCounter struct {
	n int32
}

// NewRefCounter creates a new reference counter, with an initial
// refcount of 1.
func NewRefCounter() *RefCounter {
	return &RefCounter{n: 1}
}

// IncRef increments the ref count.
func (c *RefCounter) IncRef() int32 {
	n := atomic.AddInt32(&c.n, 1)
	if n > 0 {
		return n
	}
	panic(fmt.Errorf("invalid ref count %d", n))
}

// DecRef decrements the ref count, and optionally executes the
// callback where applicable.
func (c *RefCounter) DecRef() int32 {
	n := atomic.AddInt32(&c.n, -1)
	if n >= 0 {
		return n
	}
	panic(fmt.Errorf("invalid ref count %d", n))
}

// RefCount returns the current reference count.
func (c *RefCounter) RefCount() int32 {
	return atomic.LoadInt32(&c.n)
}
