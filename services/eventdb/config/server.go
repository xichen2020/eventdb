package config

import (
	"time"

	"github.com/xichen2020/eventdb/server/http"
)

// ServerConfiguration encodes server configuraiton options.
type ServerConfiguration struct {
	Address      string         `yaml:"address"`
	WriteTimeout *time.Duration `yaml:"writeTimeout"`
	ReadTimeout  *time.Duration `yaml:"readTimeout"`
}

// NewServerOptions creates a new set of server options from the supplied config.
func (s *ServerConfiguration) NewServerOptions() http.Options {
	opts := http.NewOptions()

	if s.WriteTimeout != nil {
		opts.SetWriteTimeout(*s.WriteTimeout)
	}
	if s.ReadTimeout != nil {
		opts.SetReadTimeout(*s.ReadTimeout)
	}
	return *opts
}
