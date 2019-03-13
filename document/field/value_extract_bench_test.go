package field

import (
	"testing"

	"github.com/xichen2020/eventdb/x/bytes"
)

// Summary: Function based value extraction is ~2x faster than switch based extraction.
// Both mechanisms have 0 allocs.

var (
	benchBytesValue = ValueUnion{
		Type:     BytesType,
		BytesVal: bytes.NewImmutableBytes([]byte("foo")),
	}
	benchIntValue = ValueUnion{
		Type:   IntType,
		IntVal: 123,
	}
)

func BenchmarkBytesValueExtractionSwitch(b *testing.B) {
	benchValueExtractionSwitch(b, benchBytesValue)
}

func BenchmarkBytesValueExtractionFn(b *testing.B) {
	var val float64
	fn := func(*ValueUnion) float64 { return 0 }
	for i := 0; i < b.N; i++ {
		val = fn(&benchBytesValue)
	}
	_ = val
}

func BenchmarkIntValueExtractionSwitch(b *testing.B) {
	benchValueExtractionSwitch(b, benchBytesValue)
}

func BenchmarkIntValueExtractionFn(b *testing.B) {
	var val float64
	fn := func(v *ValueUnion) float64 { return float64(v.IntVal) }
	for i := 0; i < b.N; i++ {
		val = fn(&benchIntValue)
	}
	_ = val
}

func benchValueExtractionSwitch(b *testing.B, v ValueUnion) {
	var val float64
	for i := 0; i < b.N; i++ {
		switch v.Type {
		case IntType:
			val = float64(v.IntVal)
		case DoubleType:
			val = v.DoubleVal
		case TimeType:
			val = float64(v.TimeNanosVal)
		default:
			val = 0.0
		}
	}
	_ = val
}
