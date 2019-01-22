package integration

import (
	"github.com/xichen2020/eventdb/services/eventdb/config"
)

// configuration wraps config.Configuration with extra fields necessary for integration testing.
type configuration struct {
	// HTTP server configuration.
	HTTP config.HTTPServerConfiguration `yaml:"http"`

	// Database configuration.
	Database config.DatabaseConfiguration `yaml:"database"`

	// InputFname is the file name for the data that will be written to the database.
	InputFname string `yaml:"input"`
}
