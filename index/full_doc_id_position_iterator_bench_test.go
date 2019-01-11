package index

import (
	"testing"
)

// Summary: The custom full doc ID position iterator is 13x faster than the default
// doc ID position iterator implementation with fewer memory allocations.
var (
	benchNumTotalDocs = 2 * 1024 * 1024
	benchDocID        int32
	benchPos          int
)

func BenchmarkDefaultFullSetDocIDPositionIterator(b *testing.B) {
	fullDocIDSet := newFullDocIDSet(int32(benchNumTotalDocs))
	ds := initBenchDocIDSet(benchNumTotalDocs, 10)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fullDocSetIt := fullDocIDSet.Iter()
		maskingIt := newArrayBasedDocIDSetIterator(ds)
		it := NewDocIDPositionIterator(fullDocSetIt, maskingIt)
		var (
			docID int32
			pos   int
		)
		for it.Next() {
			docID = it.DocID()
			pos = it.Position()
		}
		benchDocID = docID
		benchPos = pos
	}
}

func BenchmarkCustomFullSetDocIDPositionIterator(b *testing.B) {
	ds := initBenchDocIDSet(benchNumTotalDocs, 10)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		maskingIt := newArrayBasedDocIDSetIterator(ds)
		it := newFullDocIDPositionIterator(int32(benchNumTotalDocs), maskingIt)
		var (
			docID int32
			pos   int
		)
		for it.Next() {
			docID += it.DocID()
			pos += it.Position()
		}
		benchDocID = docID
		benchPos = pos
	}
}

type arrayBasedDocIDSetIterator struct {
	docIDs []int32

	currIdx int
}

func newArrayBasedDocIDSetIterator(docIDs []int32) *arrayBasedDocIDSetIterator {
	return &arrayBasedDocIDSetIterator{
		docIDs:  docIDs,
		currIdx: -1,
	}
}

func (it *arrayBasedDocIDSetIterator) Next() bool {
	if it.currIdx >= len(it.docIDs) {
		return false
	}
	it.currIdx++
	return it.currIdx < len(it.docIDs)
}

func (it *arrayBasedDocIDSetIterator) DocID() int32 { return it.docIDs[it.currIdx] }

func (it *arrayBasedDocIDSetIterator) Close() { it.docIDs = nil }

func initBenchDocIDSet(n int, everyN int) []int32 {
	var arr []int32
	for i := 0; i < n; i += everyN {
		arr = append(arr, int32(i))
	}
	return arr
}
