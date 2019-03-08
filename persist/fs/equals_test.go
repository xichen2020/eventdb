package fs

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	indexfield "github.com/xichen2020/eventdb/index/field"
)

func stringFieldEquals(t *testing.T, f1, f2 indexfield.BytesField) bool {
	// Asserts that two docs fields have equal values.
	iter1, err := f1.Values().Iter()
	require.NoError(t, err)

	iter2, err := f2.Values().Iter()
	require.NoError(t, err)

	for iter1.Next() && iter2.Next() {
		if !bytes.Equal(iter1.Current().Data, iter2.Current().Data) {
			return false
		}
	}
	require.NoError(t, iter1.Err())
	require.NoError(t, iter2.Err())

	// If either iterator has extra data, they are not equal.
	if iter1.Next() || iter2.Next() {
		return false
	}
	return true
}

func intFieldEquals(t *testing.T, f1, f2 indexfield.IntField) bool {
	// Asserts that two docs fields have equal values.
	iter1, err := f1.Values().Iter()
	require.NoError(t, err)

	iter2, err := f2.Values().Iter()
	require.NoError(t, err)

	for iter1.Next() && iter2.Next() {
		if iter1.Current() != iter2.Current() {
			return false
		}
	}
	require.NoError(t, iter1.Err())
	require.NoError(t, iter2.Err())

	// If either iterator has extra data, they are not equal.
	if iter1.Next() || iter2.Next() {
		return false
	}
	return true
}

func boolFieldEquals(t *testing.T, f1, f2 indexfield.BoolField) bool {
	// Asserts that two docs fields have equal values.
	iter1, err := f1.Values().Iter()
	require.NoError(t, err)

	iter2, err := f2.Values().Iter()
	require.NoError(t, err)

	for iter1.Next() && iter2.Next() {
		if iter1.Current() != iter2.Current() {
			return false
		}
	}
	require.NoError(t, iter1.Err())
	require.NoError(t, iter2.Err())

	// If either iterator has extra data, they are not equal.
	if iter1.Next() || iter2.Next() {
		return false
	}
	return true
}

func timeFieldEquals(t *testing.T, f1, f2 indexfield.TimeField) bool {
	// Asserts that two docs fields have equal values.
	iter1, err := f1.Values().Iter()
	require.NoError(t, err)

	iter2, err := f2.Values().Iter()
	require.NoError(t, err)

	for iter1.Next() && iter2.Next() {
		if iter1.Current() != iter2.Current() {
			return false
		}
	}
	require.NoError(t, iter1.Err())
	require.NoError(t, iter2.Err())

	// If either iterator has extra data, they are not equal.
	if iter1.Next() || iter2.Next() {
		return false
	}
	return true
}
