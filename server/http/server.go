package http

import (
	"net"
	"net/http"

	"github.com/xichen2020/eventdb/server/http/handlers"

	"github.com/m3db/m3x/pprof"
	xserver "github.com/m3db/m3x/server"
)

// server is an http server.
type server struct {
	opts     *Options
	address  string
	listener net.Listener
	service  handlers.Service
}

// NewServer creates a new http server.
func NewServer(address string, svc handlers.Service, opts *Options) xserver.Server {
	if opts == nil {
		opts = NewOptions()
	}
	return &server{
		opts:    opts,
		address: address,
		service: svc,
	}
}

func (s *server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	return s.Serve(listener)
}

func (s *server) Serve(l net.Listener) error {
	mux := http.NewServeMux()
	handlers.RegisterService(mux, s.service)
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
