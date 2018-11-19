package config

// Config holds all eventdb config options.
type Config struct {
	Storage StorageConfig `yaml:"storage"`
	Server  ServerConfig  `yaml:"server"`
}

// StorageConfig holds storage options.
type StorageConfig struct{}

// ServerConfig holds server options.
type ServerConfig struct {
	Address string `yaml:"address"`
}
