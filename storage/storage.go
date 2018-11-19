package storage

// Storage exposes methods to write and query data.
type Storage interface {
	WriteBatch([][]byte) error
	Write([]byte) error
	// TODO: Implement query package and then method.
	//Query()
}

// Store implements the Storage interface.
type Store struct {
	opts Options
}

// New reutrns a new database instance.
func New(opts Options) *Store {
	return &Store{
		opts: opts,
	}
}

// WriteBatch persists multiple events.
func (s *Store) WriteBatch(events [][]byte) error {
	return nil
}

// Write persists a single event.
func (s *Store) Write(event []byte) error {
	return s.WriteBatch([][]byte{event})
}
