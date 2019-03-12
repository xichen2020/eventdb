package segment

// Metadata stores the segment metadata.
type Metadata struct {
	ID           string
	NumDocs      int32
	NumFields    int
	MinTimeNanos int64
	MaxTimeNanos int64
}
