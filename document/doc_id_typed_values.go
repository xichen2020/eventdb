package document

import "github.com/xichen2020/eventdb/encoding"

type docIDSetBuilderWithNullValues struct {
	docIDs DocIDSetBuilder
}

func newDocIDSetBuilderWithNullValues(docIDs DocIDSetBuilder) *docIDSetBuilderWithNullValues {
	return &docIDSetBuilderWithNullValues{docIDs: docIDs}
}

func (b *docIDSetBuilderWithNullValues) Add(docID int32) {
	b.docIDs.Add(docID)
}

func (b *docIDSetBuilderWithNullValues) Build(numTotalDocs int) *docIDSetWithNullValuesIter {
	return &docIDSetWithNullValuesIter{docIDSet: b.docIDs.Build(numTotalDocs)}
}

func (b *docIDSetBuilderWithNullValues) Close() {}

type docIDSetWithNullValuesIter struct {
	docIDSet DocIDSet
}

type docIDSetBuilderWithBoolValues struct {
	docIDs DocIDSetBuilder
	values boolValues
}

func newDocIDSetBuilderWithBoolValues(
	docIDs DocIDSetBuilder,
	values boolValues,
) *docIDSetBuilderWithBoolValues {
	return &docIDSetBuilderWithBoolValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithBoolValues) Add(docID int32, v bool) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithBoolValues) Build(numTotalDocs int) *docIDSetWithBoolValuesIter {
	return &docIDSetWithBoolValuesIter{
		docIDSet:  b.docIDs.Build(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithBoolValues) Close() { b.values.Close() }

type docIDSetWithBoolValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableBoolIterator
}

type docIDSetBuilderWithIntValues struct {
	docIDs DocIDSetBuilder
	values intValues
}

func newDocIDSetBuilderWithIntValues(
	docIDs DocIDSetBuilder,
	values intValues,
) *docIDSetBuilderWithIntValues {
	return &docIDSetBuilderWithIntValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithIntValues) Add(docID int32, v int) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithIntValues) Build(numTotalDocs int) *docIDSetWithIntValuesIter {
	return &docIDSetWithIntValuesIter{
		docIDSet:  b.docIDs.Build(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithIntValues) Close() { b.values.Close() }

type docIDSetWithIntValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableIntIterator
}

type docIDSetBuilderWithDoubleValues struct {
	docIDs DocIDSetBuilder
	values doubleValues
}

func newDocIDSetBuilderWithDoubleValues(
	docIDs DocIDSetBuilder,
	values doubleValues,
) *docIDSetBuilderWithDoubleValues {
	return &docIDSetBuilderWithDoubleValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithDoubleValues) Add(docID int32, v float64) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithDoubleValues) Build(numTotalDocs int) *docIDSetWithDoubleValuesIter {
	return &docIDSetWithDoubleValuesIter{
		docIDSet:  b.docIDs.Build(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithDoubleValues) Close() { b.values.Close() }

type docIDSetWithDoubleValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableDoubleIterator
}

type docIDSetBuilderWithStringValues struct {
	docIDs DocIDSetBuilder
	values stringValues
}

func newDocIDSetBuilderWithStringValues(
	docIDs DocIDSetBuilder,
	values stringValues,
) *docIDSetBuilderWithStringValues {
	return &docIDSetBuilderWithStringValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithStringValues) Add(docID int32, v string) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithStringValues) Build(numTotalDocs int) *docIDSetWithStringValuesIter {
	return &docIDSetWithStringValuesIter{
		docIDSet:  b.docIDs.Build(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithStringValues) Close() { b.values.Close() }

type docIDSetWithStringValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableStringIterator
}

type docIDSetBuilderWithTimeValues struct {
	docIDs DocIDSetBuilder
	values timeValues
}

func newDocIDSetBuilderWithTimeValues(
	docIDs DocIDSetBuilder,
	values timeValues,
) *docIDSetBuilderWithTimeValues {
	return &docIDSetBuilderWithTimeValues{docIDs: docIDs, values: values}
}

func (b *docIDSetBuilderWithTimeValues) Add(docID int32, v int64) {
	b.docIDs.Add(docID)
	b.values.Add(v)
}

func (b *docIDSetBuilderWithTimeValues) Build(numTotalDocs int) *docIDSetWithTimeValuesIter {
	return &docIDSetWithTimeValuesIter{
		docIDSet:  b.docIDs.Build(numTotalDocs),
		valueIter: b.values.Iter(),
	}
}

func (b *docIDSetBuilderWithTimeValues) Close() { b.values.Close() }

type docIDSetWithTimeValuesIter struct {
	docIDSet  DocIDSet
	valueIter encoding.RewindableTimeIterator
}
