package executor

import (
	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/index/segment"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/x/hash"
)

func extractFieldsForQuery(
	seg segment.ImmutableSegment,
	numFieldsForQuery int,
	fieldConstraints map[hash.Hash]query.FieldMeta,
) (
	allowedFieldTypes []field.ValueTypeSet,
	fieldIndexMap []int,
	fieldsToRetrieve []persist.RetrieveFieldOptions,
	err error,
) {
	// Intersect the allowed field types determined from the given query
	// with the available field types in the segment.
	fieldMap := intersectWithAvailableTypes(seg, fieldConstraints)

	// Flatten the list of fields.
	allowedFieldTypes = make([]field.ValueTypeSet, numFieldsForQuery)
	fieldIndexMap = make([]int, numFieldsForQuery)
	fieldsToRetrieve = make([]persist.RetrieveFieldOptions, 0, len(fieldMap))
	fieldIndex := 0
	for _, f := range fieldMap {
		allAllowedTypes := make(field.ValueTypeSet)
		for sourceIdx, types := range f.AllowedTypesBySourceIdx {
			allowedFieldTypes[sourceIdx] = types
			fieldIndexMap[sourceIdx] = fieldIndex
			for t := range types {
				allAllowedTypes[t] = struct{}{}
			}
		}
		fieldOpts := persist.RetrieveFieldOptions{
			FieldPath:  f.FieldPath,
			FieldTypes: allAllowedTypes,
		}
		fieldsToRetrieve = append(fieldsToRetrieve, fieldOpts)
		fieldIndex++
	}
	return allowedFieldTypes, fieldIndexMap, fieldsToRetrieve, nil
}

// intersectWithAvailableTypes intersects the allowed field types derived from
// the parsed raw query with the set of available field types in the segment,
// returning mappings from the field hashes to the set of allowed and available field types.
func intersectWithAvailableTypes(
	seg segment.ImmutableSegment,
	fieldConstraints map[hash.Hash]query.FieldMeta,
) map[hash.Hash]query.FieldMeta {
	clonedFieldMap := make(map[hash.Hash]query.FieldMeta, len(fieldConstraints))
	for k, meta := range fieldConstraints {
		clonedMeta := query.FieldMeta{
			FieldPath:               meta.FieldPath,
			AllowedTypesBySourceIdx: make(map[int]field.ValueTypeSet, len(meta.AllowedTypesBySourceIdx)),
		}
		var availableTypes []field.ValueType
		f, exists := seg.FieldAt(meta.FieldPath)
		if exists {
			availableTypes = f.Types()
		}
		for srcIdx, allowedTypes := range meta.AllowedTypesBySourceIdx {
			intersectedTypes := intersectFieldTypes(availableTypes, allowedTypes)
			clonedMeta.AllowedTypesBySourceIdx[srcIdx] = intersectedTypes
		}
		clonedFieldMap[k] = clonedMeta
	}
	return clonedFieldMap
}

func intersectFieldTypes(
	first []field.ValueType,
	second field.ValueTypeSet,
) field.ValueTypeSet {
	if len(first) == 0 || len(second) == 0 {
		return nil
	}
	res := make(field.ValueTypeSet, len(first))
	for _, t := range first {
		_, exists := second[t]
		if !exists {
			continue
		}
		res[t] = struct{}{}
	}
	return res
}

// collectFields returns the set of field for a list of field retrieving options.
// If no error is returned, the result array contains the same number of slots
// as the number of fields to retrieve. Fields that don't exist in the segment
// will have a nil slot.
func collectFields(
	seg segment.ImmutableSegment,
	retriever FieldRetriever,
	fields []persist.RetrieveFieldOptions,
) ([]indexfield.DocsField, error) {
	// Gather fields that are already loaded and identify fields that need to be retrieved
	// from filesystem.
	// NB: If no error, len(allFields) == len(fields), and `toLoad` contains all metadata
	// for fields that need to be loaded from disk.
	allFields, fieldsToRetrieve, retrievedFieldIndices, err := lookupFields(seg, fields)
	if err != nil {
		return nil, err
	}

	if len(fieldsToRetrieve) == 0 {
		return allFields, nil
	}

	// If no error, len(retrieved) == len(fieldsToRetrieve).
	retrieved, err := retriever.Retrieve(seg, fieldsToRetrieve)
	if err != nil {
		for i := range allFields {
			if allFields[i] != nil {
				allFields[i].Close()
				allFields[i] = nil
			}
		}
		return nil, err
	}

	for i, fieldIndex := range retrievedFieldIndices {
		if allFields[fieldIndex] == nil {
			// All types are retrieved from filesystem.
			allFields[fieldIndex] = retrieved[i]
		} else {
			// Some types are retrieved from memory, and others are retrieved from filesystem
			// so they need to be merged.
			merged := allFields[fieldIndex].NewMergedDocsField(retrieved[i])
			allFields[fieldIndex].Close()
			allFields[fieldIndex] = merged
			retrieved[i].Close()
			retrieved[i] = nil
		}
	}
	return allFields, nil
}

// NB(xichen): If needed, we could keep track of the fields that are currently
// being loaded (by other read requests) so that we don't load the same fields
// multiple times concurrently. This however only happens if there are simultaneous
// requests reading the same field at almost exactly the same time and therefore
// should be a relatively rare case so keeping the logic simple for now.
//
// Postcondition: If no error, `allFields` contains and owns the fields present in memory,
// and should be closed when processing is done. However, it is possible that some of
// the slots in `allFields` are nil if the corresponding fields don't exist in the segment.
// Otherwise if an error is returned, there is no need to close the field in `allFields` as
// that has been taken care of.
func lookupFields(
	seg segment.ImmutableSegment,
	fields []persist.RetrieveFieldOptions,
) (
	allFields []indexfield.DocsField,
	toRetrieveOpts []persist.RetrieveFieldOptions,
	toRetrieveFieldIndices []int,
	err error,
) {
	if len(fields) == 0 {
		return nil, nil, nil, nil
	}

	allFields = make([]indexfield.DocsField, len(fields))
	toRetrieveOpts = make([]persist.RetrieveFieldOptions, 0, len(fields))
	toRetrieveFieldIndices = make([]int, 0, len(fields))
	for i, f := range fields {
		segField, exists := seg.FieldAt(f.FieldPath)
		if !exists {
			// If the field is not present in the field map, it means this field does not
			// belong to the segment and there is no need to attempt to load it from disk.
			continue
		}

		docsField := segField.DocsField()
		if docsField == nil {
			// The docs field has been offloaded to disk, so we need to reload it.
			toRetrieveOpts = append(toRetrieveOpts, f)
			toRetrieveFieldIndices = append(toRetrieveFieldIndices, i)
			continue
		}

		// Determine if the field has all the types needed to be retrieved. The types
		// to retrieve are guaranteed to be a subset of field types available.
		retrieved, remainder, err := docsField.NewDocsFieldFor(f.FieldTypes)
		docsField.Close()
		if err != nil {
			// Close all the fields gathered so far.
			for idx := 0; idx < i; idx++ {
				if allFields[idx] != nil {
					allFields[idx].Close()
					allFields[idx] = nil
				}
			}
			return nil, nil, nil, err
		}
		allFields[i] = retrieved
		if len(remainder) == 0 {
			// All types to retrieve have been retrieved.
			continue
		}

		// Still more types to retrieve.
		retrieveOpts := persist.RetrieveFieldOptions{
			FieldPath:  f.FieldPath,
			FieldTypes: remainder,
		}
		toRetrieveOpts = append(toRetrieveOpts, retrieveOpts)
		toRetrieveFieldIndices = append(toRetrieveFieldIndices, i)
	}

	return allFields, toRetrieveOpts, toRetrieveFieldIndices, nil
}
