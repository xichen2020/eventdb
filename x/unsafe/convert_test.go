package unsafe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToString(t *testing.T) {
	inputs := [][]byte{
		[]byte("abc"),
		[]byte("asdfaslkdfalewiruaoiejhfliajsdflkajsldfjalsf"),
		[]byte(""),
	}

	for i := range inputs {
		require.Equal(t, string(inputs[i]), ToString(inputs[i]))
	}
}
