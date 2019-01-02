package refcnt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefCounter(t *testing.T) {
	var called bool
	callbackFn := func() { called = true }

	c := NewRefCounter(callbackFn)
	require.Equal(t, int32(1), c.n)
	require.False(t, called)

	require.NotPanics(t, func() { c.DecRef() })
	require.Equal(t, int32(0), c.n)
	require.True(t, called)
}

func TestRefCounterInvalidRefCountPanics(t *testing.T) {
	b := NewRefCounter(nil)
	require.NotPanics(t, func() { b.DecRef() })
	require.Panics(t, func() { b.DecRef() })
}
