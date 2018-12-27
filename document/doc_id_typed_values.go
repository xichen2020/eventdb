package document

import "github.com/xichen2020/eventdb/encoding"

type docIDSetBuilderWithNullValues struct {
	docIDs docIDSetBuilder
}

func newDocIDSetBuilderWithNullValues(docIDs docIDSetBuilder) *docIDSetBuilderWithNullValues {
	return &docIDSetBuilderWithNullValues{docIDs: docIDs}
}

func (b *docIDSetBuilderWithNullValues) Add(docID int32) {
	b.docIDs.Add(docID)
}

func (b *docIDSetBuilderWithNullValues) Snapshot() *docIDSetWithNullValuesIter {
	return &docIDSetWithNullValuesIter{docIDSet: b.docIDs.Snapshot()}
}

func (b *docIDSetBuilderWithNullValues) Seal(numTotalDocs int) *docIDSetWithNullValuesIter {
	return &docIDSetWithNullValuesIter{docIDSet: b.docIDs.Seal(numTotalDocs)}
}

func (b *docIDSetBuilderWithNullValues) Close() {}

type docIDSetWithNullValuesIter struct {
	docIDSet DocIDSet
}

type docIDSetBuilderWithBoolValues struct {
	docIDs docIDSetBuilder
	values boolValues
}

func newDocIDSetBuilderWithBoolValues(
	docIDs docIDSetBuilder,
	values boolValues,
) *docIDSetBuilderWithBoolValues {
	return &docIDSetBuilderWithBoolValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithBoolValues) Add(docID int32, v bool) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithBoolValues) Snapshot() *docIDSetWithBoolValuesIter {
	return &docIDSetWithBoolValuesIter{
		docIDSet:  b.docIDs.Snapshot(),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithBoolValues) Seal(numTotalDocs int) *docIDSetWithBoolValuesIter {
	return &docIDSetWithBoolValuesIter{
		docIDSet:  b.docIDs.Seal(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithBoolValues) Close() { b.values.Close() }

type docIDSetWithBoolValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableBoolIterator
}

type docIDSetBuilderWithIntValues struct {
	docIDs docIDSetBuilder
	values intValues
}

func newDocIDSetBuilderWithIntValues(
	docIDs docIDSetBuilder,
	values intValues,
) *docIDSetBuilderWithIntValues {
	return &docIDSetBuilderWithIntValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithIntValues) Add(docID int32, v int) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithIntValues) Snapshot() *docIDSetWithIntValuesIter {
	return &docIDSetWithIntValuesIter{
		docIDSet:  b.docIDs.Snapshot(),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithIntValues) Seal(numTotalDocs int) *docIDSetWithIntValuesIter {
	return &docIDSetWithIntValuesIter{
		docIDSet:  b.docIDs.Seal(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithIntValues) Close() { b.values.Close() }

type docIDSetWithIntValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableIntIterator
}

type docIDSetBuilderWithDoubleValues struct {
	docIDs docIDSetBuilder
	values doubleValues
}

func newDocIDSetBuilderWithDoubleValues(
	docIDs docIDSetBuilder,
	values doubleValues,
) *docIDSetBuilderWithDoubleValues {
	return &docIDSetBuilderWithDoubleValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithDoubleValues) Add(docID int32, v float64) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithDoubleValues) Snapshot() *docIDSetWithDoubleValuesIter {
	return &docIDSetWithDoubleValuesIter{
		docIDSet:  b.docIDs.Snapshot(),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithDoubleValues) Seal(numTotalDocs int) *docIDSetWithDoubleValuesIter {
	return &docIDSetWithDoubleValuesIter{
		docIDSet:  b.docIDs.Seal(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithDoubleValues) Close() { b.values.Close() }

type docIDSetWithDoubleValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableDoubleIterator
}

type docIDSetBuilderWithStringValues struct {
	docIDs docIDSetBuilder
	values stringValues
}

func newDocIDSetBuilderWithStringValues(
	docIDs docIDSetBuilder,
	values stringValues,
) *docIDSetBuilderWithStringValues {
	return &docIDSetBuilderWithStringValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithStringValues) Add(docID int32, v string) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithStringValues) Snapshot() *docIDSetWithStringValuesIter {
	return &docIDSetWithStringValuesIter{
		docIDSet:  b.docIDs.Snapshot(),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithStringValues) Seal(numTotalDocs int) *docIDSetWithStringValuesIter {
	return &docIDSetWithStringValuesIter{
		docIDSet:  b.docIDs.Seal(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithStringValues) Close() { b.values.Close() }

type docIDSetWithStringValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableStringIterator
}

type docIDSetBuilderWithTimeValues struct {
	docIDs docIDSetBuilder
	values timeValues
}

func newDocIDSetBuilderWithTimeValues(
	docIDs docIDSetBuilder,
	values timeValues,
) *docIDSetBuilderWithTimeValues {
	return &docIDSetBuilderWithTimeValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithTimeValues) Add(docID int32, v int64) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithTimeValues) Snapshot() *docIDSetWithTimeValuesIter {
	return &docIDSetWithTimeValuesIter{
		docIDSet:  b.docIDs.Snapshot(),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithTimeValues) Seal(numTotalDocs int) *docIDSetWithTimeValuesIter {
	return &docIDSetWithTimeValuesIter{
		docIDSet:  b.docIDs.Seal(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithTimeValues) Close() { b.values.Close() }

type docIDSetWithTimeValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableTimeIterator
}
