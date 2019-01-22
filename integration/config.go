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

	// Integration testing specific configuration.
	Integration integrationConfiguration `yaml:"integration"`
}

type integrationConfiguration struct {
	InputFname string                     `yaml:"input"`
	Queries    []queryAndExpectedResponse `yaml:"queries"`
}

type queryAndExpectedResponse struct {
	Query    string `yaml:"query"`
	Response string `yaml:"response"`
}
