package decoding

import "errors"

// Shared errors in the decoding package.
var (
	errNilFilterValue            = errors.New("nil filter value")
	errUnexpectedFilterValueType = errors.New("unexpected filter value type")
)
