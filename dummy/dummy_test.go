package dummy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFoo(t *testing.T) {
	require.Equal(t, 1, foo())
}