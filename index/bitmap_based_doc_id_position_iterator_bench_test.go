package index

import (
	"testing"

	"github.com/pilosa/pilosa/roaring"
)

// Summary: The default doc ID position iterator is as fast as the custom one, with
// fewer memory allocations.

func BenchmarkDefaultBitmapDocIDPositionIterator(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 5)
	ds := initBenchDocIDSet(benchNumTotalDocs, 8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bmIt := newBitmapBasedDocIDSet(bm).Iter()
		dsIt := newArrayBasedDocIDSetIterator(ds)
		defaultIt := NewDocIDPositionIterator(bmIt, dsIt)
		count := 0
		for defaultIt.Next() {
			benchDocID = defaultIt.DocID()
			benchPos = defaultIt.Position()
			count++
		}
	}
}

func BenchmarkCustomBitmapDocIDPositionIterator(b *testing.B) {
	bm := initBenchBitmap(benchNumTotalDocs, 5)
	ds := initBenchDocIDSet(benchNumTotalDocs, 8)

	bm.Optimize()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dsIt := newArrayBasedDocIDSetIterator(ds)
		it := newBitmapBasedDocIDPositionIterator(bm, dsIt)
		count := 0
		for it.Next() {
			benchDocID = it.DocID()
			benchPos = it.Position()
			count++
		}
	}
}

// nolint: unparam
func initBenchBitmap(n int, everyN int) *roaring.Bitmap {
	bm := roaring.NewBitmap()
	for j := 0; j < n; j++ {
		if j%everyN == 0 {
			bm.DirectAdd(uint64(j))
		}
	}
	return bm
}
