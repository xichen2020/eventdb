package schema

const (
	// SegmentVersion is the current version of segment files.
	SegmentVersion = 1
)

// DocIDSetType is the type of a doc ID set.
type DocIDSetType byte

const (
	// PartialDocIDSet is a partial doc ID set where only some of the doc
	// IDs in the segment is included in the doc ID set.
	PartialDocIDSet DocIDSetType = iota

	// FullDocIDSet is a full doc ID set where every doc ID in the segment
	// is included in the doc ID set. This corresponds to a field that is
	// present in every document (e.g., timestamp).
	FullDocIDSet
)
