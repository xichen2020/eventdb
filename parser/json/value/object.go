package value

import "strconv"

// Object is an object containing a list of key value pairs.
type Object struct {
	kvs KVArray
}

// NewObject creates a new object.
func NewObject(kvs KVArray) Object { return Object{kvs: kvs} }

// MarshalTo appends marshaled o to dst and returns the result.
func (o *Object) MarshalTo(dst []byte) ([]byte, error) {
	var err error
	dst = append(dst, '{')
	for i, kv := range o.kvs.raw {
		dst = strconv.AppendQuote(dst, kv.k)
		dst = append(dst, ':')
		dst, err = kv.v.MarshalTo(dst)
		if err != nil {
			return nil, err
		}
		if i != len(o.kvs.raw)-1 {
			dst = append(dst, ',')
		}
	}
	dst = append(dst, '}')
	return dst, nil
}

// Len returns the number of items in the object.
func (o *Object) Len() int { return len(o.kvs.raw) }

// At returns the key value pair at given index.
func (o *Object) At(i int) KV { return o.kvs.raw[i] }

// Get returns the value for the given key in the o, and a bool indicating
// whether the value is found.
func (o *Object) Get(key string) (*Value, bool) {
	for _, kv := range o.kvs.raw {
		if kv.k == key {
			return kv.v, true
		}
	}
	return nil, false
}

// Visit calls f for each item in the object in the original order.
//
// f cannot hold key and/or v after returning.
func (o Object) Visit(f func(key string, v *Value)) {
	for _, kv := range o.kvs.raw {
		f(kv.k, kv.v)
	}
}

// Close closes the object.
func (o *Object) Close() {
	o.kvs.Close()
	o.kvs = KVArray{}
}

// KV is a key value pair.
type KV struct {
	k string
	v *Value
}

// NewKV creates a new key value pair.
func NewKV(k string, v *Value) KV {
	return KV{k: k, v: v}
}

// Key returns the key.
func (kv KV) Key() string { return kv.k }

// Value returns the value.
func (kv KV) Value() *Value { return kv.v }

// KVArray is an array of key value pairs.
type KVArray struct {
	raw []KV
	p   *BucketizedKVArrayPool
}

// NewKVArray creates a new KV array.
func NewKVArray(raw []KV, p *BucketizedKVArrayPool) KVArray {
	return KVArray{raw: raw, p: p}
}

// Capacity returns the capacity of the underlying array.
func (a KVArray) Capacity() int { return cap(a.raw) }

// Len returns the number of key value pairs.
func (a KVArray) Len() int { return len(a.raw) }

// Reset reset the kv array.
func (a *KVArray) Reset() {
	for i := 0; i < len(a.raw); i++ {
		a.raw[i] = KV{}
	}
	a.raw = a.raw[:0]
}

// Append appends a key value pair to the end of the KV array.
func (a *KVArray) Append(v KV) {
	if a.p == nil || len(a.raw) < cap(a.raw) {
		a.raw = append(a.raw, v)
		return
	}

	oldCapacity := cap(a.raw)
	oldArray := *a
	*a = oldArray.p.Get(oldCapacity * 2)
	n := copy(a.raw[:oldCapacity], oldArray.raw)
	oldArray.Reset()
	oldArray.p.Put(oldArray, oldCapacity)
	a.raw = append(a.raw[:n], v)
}

// Close closes the kv array.
func (a KVArray) Close() {
	for i := range a.raw {
		a.raw[i].v.Close()
		a.raw[i].v = nil
	}
	p := a.p
	if p != nil {
		a.Reset()
		p.Put(a, cap(a.raw))
	}
}
