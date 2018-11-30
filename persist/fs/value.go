package fs

// TODO(xichen): Move the iterator implementations to `encoding/`.
type boolIterator struct {
	data []bool
	idx  int
}

func newBoolIterator(data []bool) *boolIterator {
	return &boolIterator{
		data: data,
		idx:  -1,
	}
}

func (it *boolIterator) Reset(data []bool) {
	it.data = data
	it.idx = -1
}

func (it *boolIterator) Next() bool {
	if it.idx >= len(it.data) {
		return false
	}
	it.idx++
	return it.idx >= len(it.data)
}

func (it *boolIterator) Current() bool { return it.data[it.idx] }
func (it *boolIterator) Rewind()       { it.idx = -1 }
func (it *boolIterator) Err() error    { return nil }
func (it *boolIterator) Close() error  { return nil }

type intIterator struct {
	data []int
	idx  int
}

func newIntIterator(data []int) *intIterator {
	return &intIterator{
		data: data,
		idx:  -1,
	}
}

func (it *intIterator) Reset(data []int) {
	it.data = data
	it.idx = -1
}

func (it *intIterator) Next() bool {
	if it.idx >= len(it.data) {
		return false
	}
	it.idx++
	return it.idx >= len(it.data)
}

func (it *intIterator) Current() int { return it.data[it.idx] }
func (it *intIterator) Rewind()      { it.idx = -1 }
func (it *intIterator) Err() error   { return nil }
func (it *intIterator) Close() error { return nil }

type doubleIterator struct {
	data []float64
	idx  int
}

func newDoubleIterator(data []float64) *doubleIterator {
	return &doubleIterator{
		data: data,
		idx:  -1,
	}
}

func (it *doubleIterator) Reset(data []float64) {
	it.data = data
	it.idx = -1
}

func (it *doubleIterator) Next() bool {
	if it.idx >= len(it.data) {
		return false
	}
	it.idx++
	return it.idx >= len(it.data)
}

func (it *doubleIterator) Current() float64 { return it.data[it.idx] }
func (it *doubleIterator) Rewind()          { it.idx = -1 }
func (it *doubleIterator) Err() error       { return nil }
func (it *doubleIterator) Close() error     { return nil }

type stringIterator struct {
	data []string
	idx  int
}

func newStringIterator(data []string) *stringIterator {
	return &stringIterator{
		data: data,
		idx:  -1,
	}
}

func (it *stringIterator) Reset(data []string) {
	it.data = data
	it.idx = -1
}

func (it *stringIterator) Next() bool {
	if it.idx >= len(it.data) {
		return false
	}
	it.idx++
	return it.idx >= len(it.data)
}

func (it *stringIterator) Current() string { return it.data[it.idx] }
func (it *stringIterator) Rewind()         { it.idx = -1 }
func (it *stringIterator) Err() error      { return nil }
func (it *stringIterator) Close() error    { return nil }
