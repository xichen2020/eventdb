package document

import (
	"testing"

	rroaring "github.com/RoaringBitmap/roaring"
	proaring "github.com/pilosa/pilosa/roaring"
	"github.com/willf/bitset"
)

func BenchmarkInAllDocIDSetIteratorDense(b *testing.B) {
	benchmarkInAllDocIDSetIterator(b, testDenseBenchSetup)
}

func BenchmarkDocIDSetIteratorBitsetDense(b *testing.B) {
	benchmarkDocIDSetIteratorBitset(b, testDenseBenchSetup)
}

func BenchmarkDocIDSetIteratorRRoaringBitmapDense(b *testing.B) {
	benchmarkDocIDSetIteratorRRoaringBitmap(b, testDenseBenchSetup)
}

func BenchmarkDocIDSetIteratorPilosaRoaringBitmapDense(b *testing.B) {
	benchmarkDocIDSetIteratorPilosaRoaringBitmap(b, testDenseBenchSetup)
}

func BenchmarkInAllDocIDSetIteratorSparse(b *testing.B) {
	benchmarkInAllDocIDSetIterator(b, testSparseBenchSetup)
}

func BenchmarkDocIDSetIteratorBitsetSparse(b *testing.B) {
	benchmarkDocIDSetIteratorBitset(b, testSparseBenchSetup)
}

func BenchmarkDocIDSetIteratorRRoaringBitmapSparse(b *testing.B) {
	benchmarkDocIDSetIteratorRRoaringBitmap(b, testSparseBenchSetup)
}

func BenchmarkDocIDSetIteratorPilosaRoaringBitmapSparse(b *testing.B) {
	benchmarkDocIDSetIteratorPilosaRoaringBitmap(b, testSparseBenchSetup)
}

func benchmarkInAllDocIDSetIterator(b *testing.B, ts testBenchSetup) {
	bms := initBenchBitmaps(ts)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iters := make([]DocIDSetIterator, 0, ts.numFields)
		for _, bm := range bms {
			iters = append(iters, newbitmapBasedDocIDIterator(bm.Iterator()))
		}
		inAllIter := NewInAllDocIDSetIterator(iters...)

		count := 0
		for inAllIter.Next() {
			count++
		}
	}
}

func benchmarkDocIDSetIteratorBitset(b *testing.B, ts testBenchSetup) {
	bms := initBenchBitmaps(ts)

	bs1 := bitset.New(uint(ts.totalDocs))
	bs2 := bitset.New(uint(ts.totalDocs))
	buf := make([]uint, 256)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs1.ClearAll()
		iter := bms[0].Iterator()
		for {
			v, eof := iter.Next()
			if eof {
				break
			}
			bs1.Set(uint(v))
		}

		for j := 1; j < ts.numFields; j++ {
			bs2.ClearAll()
			iter := bms[j].Iterator()
			for {
				v, eof := iter.Next()
				if eof {
					break
				}
				bs2.Set(uint(v))
			}
			bs1.InPlaceIntersection(bs2)
		}

		j := uint(0)
		j, buf = bs1.NextSetMany(j, buf)
		count := 0
		for ; len(buf) > 0; j, buf = bs1.NextSetMany(j, buf) {
			count += len(buf)
			j++
		}
	}
}

func benchmarkDocIDSetIteratorRRoaringBitmap(b *testing.B, ts testBenchSetup) {
	bms := initBenchBitmaps(ts)

	bm1 := rroaring.New()
	bm2 := rroaring.New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm1.Clear()
		iter := bms[0].Iterator()
		for {
			v, eof := iter.Next()
			if eof {
				break
			}
			bm1.Add(uint32(v))
		}

		for j := 1; j < ts.numFields; j++ {
			bm2.Clear()
			iter := bms[j].Iterator()
			for {
				v, eof := iter.Next()
				if eof {
					break
				}
				bm2.Add(uint32(v))
			}
			bm1.And(bm2)
		}

		count := 0
		it := bm1.Iterator()
		for it.HasNext() {
			count++
			it.Next()
		}
	}
}

func benchmarkDocIDSetIteratorPilosaRoaringBitmap(b *testing.B, ts testBenchSetup) {
	bms := initBenchBitmaps(ts)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm1 := proaring.NewBitmap()
		iter := bms[0].Iterator()
		for {
			v, eof := iter.Next()
			if eof {
				break
			}
			bm1.DirectAdd(uint64(v))
		}

		for j := 1; j < ts.numFields; j++ {
			bm2 := proaring.NewBitmap()
			iter := bms[j].Iterator()
			for {
				v, eof := iter.Next()
				if eof {
					break
				}
				bm2.DirectAdd(uint64(v))
			}
			bm1 = bm1.Intersect(bm2)
		}

		count := 0
		it := bm1.Iterator()
		for {
			_, eof := it.Next()
			if eof {
				break
			}
			count++
		}
	}
}

func initBenchBitmaps(ts testBenchSetup) []*proaring.Bitmap {
	bms := make([]*proaring.Bitmap, 0, ts.numFields)
	for i := 0; i < ts.numFields; i++ {
		bm := proaring.NewBitmap()
		for j := 0; j < ts.totalDocs; j++ {
			if j%ts.everyNs[i] == 0 {
				bm.DirectAdd(uint64(j))
			}
		}
		bms = append(bms, bm)
	}
	return bms
}

type testBenchSetup struct {
	numFields int
	totalDocs int
	everyNs   []int
}

var (
	testDenseBenchSetup = testBenchSetup{
		numFields: 3,
		totalDocs: 2 * 1024 * 1024,
		everyNs:   []int{3, 6, 9},
	}
	testSparseBenchSetup = testBenchSetup{
		numFields: 3,
		totalDocs: 2 * 1024 * 1024,
		everyNs:   []int{50, 500, 2000},
	}
)
