package index

import (
	"testing"

	"github.com/pilosa/pilosa/roaring"
)

func BenchmarkDenseDocIDSetSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs*2, 2)
	benchmarkDocIDSetSnapShot(b, bm)
}

func BenchmarkParseDocIDSetSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 500)
	benchmarkDocIDSetSnapShot(b, bm)
}

func benchmarkDocIDSetSnapShot(b *testing.B, bm *roaring.Bitmap) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder := NewBitmapBasedDocIDSetBuilder(bm)
		_ = builder.Snapshot()
	}
}
