package config

import (
	"github.com/xichen2020/eventdb/server"
	"github.com/xichen2020/eventdb/storage"
)

// Config holds all eventdb config options.
type Config struct {
	Storage storage.Options `yaml"storage"`
	Server  server.Options  `yaml:"server"`
}
