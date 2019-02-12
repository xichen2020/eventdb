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
	opts    *Options
	address string
	server  *http.Server
}

// NewServer creates a new http server.
func NewServer(address string, svc handlers.Service, opts *Options) xserver.Server {
	if opts == nil {
		opts = NewOptions()
	}

	mux := http.NewServeMux()
	handlers.RegisterService(mux, svc)
	pprof.RegisterHandler(mux)

	return &server{
		opts:    opts,
		address: address,
		server: &http.Server{
			Handler:      mux,
			ReadTimeout:  opts.ReadTimeout(),
			WriteTimeout: opts.WriteTimeout(),
		},
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
	go func() {
		s.server.Serve(l)
	}()

	return nil
}

func (s *server) Close() {
	if err := s.server.Close(); err != nil {
		s.opts.InstrumentOptions().Logger().Errorf("server close error %v\n", err)
	}
}
