package roundtriptest

import (
	indexfield "github.com/xichen2020/eventdb/index/field"
)

func stringFieldEquals(f1, f2 indexfield.StringField) (bool, error) {
	// Asserts that two docs fields have equal values.
	iter1, err := f1.Iter()
	if err != nil {
		return false, err
	}
	iter2, err := f2.Iter()
	if err != nil {
		return false, err
	}
	for iter1.Next() && iter2.Next() {
		if iter1.Value() != iter2.Value() {
			return false, nil
		}
	}
	return true, nil
}

func intFieldEquals(f1, f2 indexfield.IntField) (bool, error) {
	// Asserts that two docs fields have equal values.
	iter1, err := f1.Iter()
	if err != nil {
		return false, err
	}
	iter2, err := f2.Iter()
	if err != nil {
		return false, err
	}
	for iter1.Next() && iter2.Next() {
		if iter1.Value() != iter2.Value() {
			return false, nil
		}
	}
	return true, nil
}

func boolFieldEquals(f1, f2 indexfield.BoolField) (bool, error) {
	// Asserts that two docs fields have equal values.
	iter1, err := f1.Iter()
	if err != nil {
		return false, err
	}
	iter2, err := f2.Iter()
	if err != nil {
		return false, err
	}
	for iter1.Next() && iter2.Next() {
		if iter1.Value() != iter2.Value() {
			return false, nil
		}
	}
	return true, nil
}

func timeFieldEquals(f1, f2 indexfield.TimeField) (bool, error) {
	// Asserts that two docs fields have equal values.
	iter1, err := f1.Iter()
	if err != nil {
		return false, err
	}
	iter2, err := f2.Iter()
	if err != nil {
		return false, err
	}
	for iter1.Next() && iter2.Next() {
		if iter1.Value() != iter2.Value() {
			return false, nil
		}
	}
	return true, nil
}
