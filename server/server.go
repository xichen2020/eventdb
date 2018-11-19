package server

import (
	"net"
	"net/http"

	"github.com/xichen2020/eventdb/storage"

	"github.com/m3db/m3x/pprof"
	xserver "github.com/m3db/m3x/server"
)

// Server exposes http endpoints that handle reads and writes.
// Uses the default servemux under the hood.
type server struct {
	opts     Options
	address  string
	listener net.Listener
	store    storage.Storage
}

// New returns a new server instance.
func New(address string, store storage.Storage, opts Options) xserver.Server {
	return &server{
		opts:    opts,
		address: address,
		store:   store,
	}
}

// ListenAndServe runs the server.
func (s *server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

// Serve runs the server.
func (s *server) Serve(l net.Listener) error {
	mux := http.NewServeMux()
	registerHandlers(mux, s.store)
	pprof.RegisterHandler(mux)

	server := http.Server{
		Handler:      mux,
		ReadTimeout:  s.opts.ReadTimeout(),
		WriteTimeout: s.opts.WriteTimeout(),
	}

	s.listener = l
	s.address = l.Addr().String()

	go func() {
		server.Serve(l)
	}()

	return nil
}

func (s *server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
}
