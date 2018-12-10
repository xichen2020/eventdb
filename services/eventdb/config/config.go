package config

import (
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
)

// Configuration contains top-level configuration.
type Configuration struct {
	// Logging configuration.
	Logging log.Configuration `yaml:"logging"`

	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// HTTP server configuration.
	HTTP HTTPServerConfiguration `yaml:"http"`

	// Database configuration.
	Database DatabaseConfiguration `yaml:"database"`
}