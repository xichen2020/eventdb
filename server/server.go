package server

import (
	"fmt"
	"net/http"

	"github.com/xichen2020/eventdb/storage"
)

// Options for a server.
type Options struct {
	Port int
}

// Server exposes http endpoints that handle reads and writes.
// Uses the default servemux under the hood.
type Server struct {
	opts  Options
	store storage.Storage
}

// New returns a new server instance.
func New(opts Options, store storage.Storage) *Server {
	return &Server{
		opts:  opts,
		store: store,
	}
}

// Serve runs the server.
func (s *Server) Serve() error {
	return http.ListenAndServe(fmt.Sprintf(":%d", s.opts.Port), nil)
}
