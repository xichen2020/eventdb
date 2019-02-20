package join

type MyStr string

func (s MyStr) String() string {
	return string(s)
}
