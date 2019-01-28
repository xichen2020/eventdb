package query

// InsertionStatus represents an insertion status.
type InsertionStatus int

// A list of supported insertion status.
const (
	Existent InsertionStatus = iota
	Inserted
	RejectedDueToLimit
)
