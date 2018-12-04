package config

// Config holds all eventdb config options.
type Config struct {
	Database DatabaseConfiguration `yaml:"database"`
	Server   ServerConfiguration   `yaml:"server"`
}
