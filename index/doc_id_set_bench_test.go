package index

import (
	"testing"

	"github.com/pilosa/pilosa/roaring"
)

func BenchmarkDenseDocIDSetCloneSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 1)
	benchmarkDocIDSetCloneSnapshot(b, bm)
}

func BenchmarkDenseDocIDSetCheapSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 1)
	benchmarkDocIDSetCheapSnapshot(b, bm)
}

func BenchmarkSparseDocIDSetCloneSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 500)
	benchmarkDocIDSetCloneSnapshot(b, bm)
}

func BenchmarkSparseDocIDSetCheapSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 500)
	benchmarkDocIDSetCheapSnapshot(b, bm)
}

func benchmarkDocIDSetCloneSnapshot(b *testing.B, bm *roaring.Bitmap) {
	builder := NewBitmapBasedDocIDSetBuilder(bm)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = builder.Snapshot()
	}
}

func benchmarkDocIDSetCheapSnapshot(b *testing.B, bm *roaring.Bitmap) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = newBitmapBasedDocIDSet(bm)
	}
}
