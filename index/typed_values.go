package index

import (
	"github.com/xichen2020/eventdb/encoding"
	"github.com/xichen2020/eventdb/x/pool"
)

// TODO(xichen): Investigate more compact encoding of the values for memory efficiency.
type boolValues interface {
	Add(v bool)

	// NB: Iter provides access to the snapshot of the underlying dataset when the iterator
	// is created. After the iterator is returned, the iterator has no access to future values
	// added to the underlying dataset. The returned dataset should remain valid for as long
	// as the underlying values remain valid.
	Iter() encoding.RewindableBoolIterator

	Close()
}

type arrayBasedBoolValues struct {
	p    *pool.BucketizedBoolArrayPool
	vals []bool
}

func newArrayBasedBoolValues(p *pool.BucketizedBoolArrayPool) *arrayBasedBoolValues {
	return &arrayBasedBoolValues{
		p:    p,
		vals: p.Get(defaultInitialFieldValuesCapacity),
	}
}

func (b *arrayBasedBoolValues) Add(v bool) {
	b.vals = pool.AppendBool(b.vals, v, b.p)
}

func (b *arrayBasedBoolValues) Iter() encoding.RewindableBoolIterator {
	return encoding.NewArrayBasedBoolIterator(b.vals)
}

func (b *arrayBasedBoolValues) Close() {
	b.vals = b.vals[:0]
	b.p.Put(b.vals, cap(b.vals))
	b.vals = nil
}

type intValues interface {
	Add(v int)

	// NB: Iter provides access to the snapshot of the underlying dataset when the iterator
	// is created. After the iterator is returned, the iterator has no access to future values
	// added to the underlying dataset. The returned dataset should remain valid for as long
	// as the underlying values remain valid.
	Iter() encoding.RewindableIntIterator

	Close()
}

type arrayBasedIntValues struct {
	p    *pool.BucketizedIntArrayPool
	vals []int
}

func newArrayBasedIntValues(p *pool.BucketizedIntArrayPool) *arrayBasedIntValues {
	return &arrayBasedIntValues{
		p:    p,
		vals: p.Get(defaultInitialFieldValuesCapacity),
	}
}

func (b *arrayBasedIntValues) Add(v int) {
	b.vals = pool.AppendInt(b.vals, v, b.p)
}

func (b *arrayBasedIntValues) Iter() encoding.RewindableIntIterator {
	return encoding.NewArrayBasedIntIterator(b.vals)
}

func (b *arrayBasedIntValues) Close() {
	b.vals = b.vals[:0]
	b.p.Put(b.vals, cap(b.vals))
	b.vals = nil
}

type doubleValues interface {
	Add(v float64)

	// NB: Iter provides access to the snapshot of the underlying dataset when the iterator
	// is created. After the iterator is returned, the iterator has no access to future values
	// added to the underlying dataset. The returned dataset should remain valid for as long
	// as the underlying values remain valid.
	Iter() encoding.RewindableDoubleIterator

	Close()
}

type arrayBasedDoubleValues struct {
	p    *pool.BucketizedFloat64ArrayPool
	vals []float64
}

func newArrayBasedDoubleValues(p *pool.BucketizedFloat64ArrayPool) *arrayBasedDoubleValues {
	return &arrayBasedDoubleValues{
		p:    p,
		vals: p.Get(defaultInitialFieldValuesCapacity),
	}
}

func (b *arrayBasedDoubleValues) Add(v float64) {
	b.vals = pool.AppendFloat64(b.vals, v, b.p)
}

func (b *arrayBasedDoubleValues) Iter() encoding.RewindableDoubleIterator {
	return encoding.NewArrayBasedDoubleIterator(b.vals)
}

func (b *arrayBasedDoubleValues) Close() {
	b.vals = b.vals[:0]
	b.p.Put(b.vals, cap(b.vals))
	b.vals = nil
}

type stringValues interface {
	Add(v string)

	// NB: Iter provides access to the snapshot of the underlying dataset when the iterator
	// is created. After the iterator is returned, the iterator has no access to future values
	// added to the underlying dataset. The returned dataset should remain valid for as long
	// as the underlying values remain valid.
	Iter() encoding.RewindableStringIterator

	Close()
}

type arrayBasedStringValues struct {
	p    *pool.BucketizedStringArrayPool
	vals []string
}

func newArrayBasedStringValues(p *pool.BucketizedStringArrayPool) *arrayBasedStringValues {
	return &arrayBasedStringValues{
		p:    p,
		vals: p.Get(defaultInitialFieldValuesCapacity),
	}
}

func (b *arrayBasedStringValues) Add(v string) {
	b.vals = pool.AppendString(b.vals, v, b.p)
}

func (b *arrayBasedStringValues) Iter() encoding.RewindableStringIterator {
	return encoding.NewArrayBasedStringIterator(b.vals)
}

func (b *arrayBasedStringValues) Close() {
	b.vals = b.vals[:0]
	b.p.Put(b.vals, cap(b.vals))
	b.vals = nil
}

type timeValues interface {
	Add(v int64)

	// NB: Iter provides access to the snapshot of the underlying dataset when the iterator
	// is created. After the iterator is returned, the iterator has no access to future values
	// added to the underlying dataset. The returned dataset should remain valid for as long
	// as the underlying values remain valid.
	Iter() encoding.RewindableTimeIterator

	Close()
}

type arrayBasedTimeValues struct {
	p    *pool.BucketizedInt64ArrayPool
	vals []int64
}

func newArrayBasedTimeValues(p *pool.BucketizedInt64ArrayPool) *arrayBasedTimeValues {
	return &arrayBasedTimeValues{
		p:    p,
		vals: p.Get(defaultInitialFieldValuesCapacity),
	}
}

func (b *arrayBasedTimeValues) Add(v int64) {
	b.vals = pool.AppendInt64(b.vals, v, b.p)
}

func (b *arrayBasedTimeValues) Iter() encoding.RewindableTimeIterator {
	return encoding.NewArrayBasedTimeIterator(b.vals)
}

func (b *arrayBasedTimeValues) Close() {
	b.vals = b.vals[:0]
	b.p.Put(b.vals, cap(b.vals))
	b.vals = nil
}
