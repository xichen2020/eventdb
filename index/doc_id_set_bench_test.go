package index

import (
	"testing"

	"github.com/pilosa/pilosa/roaring"
)

/*
Summary: Snapshot of dense bitmap takes 45us. Gets cheaper the sparser the dataset.

BenchmarkFullDocIDSetCloneSnapshot-8      	   30000	     44430 ns/op	  265336 B/op	      69 allocs/op
BenchmarkDenseDocIDSetCloneSnapshotV2-8   	   30000	     48289 ns/op	  265336 B/op	      69 allocs/op
BenchmarkSparseDocIDSetCloneSnapshot-8    	  500000	      4085 ns/op	   12408 B/op	      69 allocs/op
BenchmarkDocIDSetCheapSnapshot-8          	2000000000	         0.29 ns/op	       0 B/op	       0 allocs/op
*/

func BenchmarkFullDocIDSetCloneSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 1)
	benchmarkDocIDSetCloneSnapshot(b, bm)
}

func BenchmarkDenseDocIDSetCloneSnapshotV2(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 3)
	benchmarkDocIDSetCloneSnapshot(b, bm)
}

func BenchmarkSparseDocIDSetCloneSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 500)
	benchmarkDocIDSetCloneSnapshot(b, bm)
}

func BenchmarkDocIDSetCheapSnapshot(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 1)
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
		// TODO(wjang): This is an approximation of the cloning we'll be doing.
		_ = newBitmapBasedDocIDSet(bm)
	}
}
