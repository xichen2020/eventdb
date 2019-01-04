package refcnt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefCounter(t *testing.T) {
	c := NewRefCounter()
	require.Equal(t, int32(1), c.n)

	require.NotPanics(t, func() { c.DecRef() })
	require.Equal(t, int32(0), c.n)
}

func TestRefCounterInvalidRefCountPanics(t *testing.T) {
	b := NewRefCounter()
	require.NotPanics(t, func() { b.DecRef() })
	require.Panics(t, func() { b.DecRef() })
}
