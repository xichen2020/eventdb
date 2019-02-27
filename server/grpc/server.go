package grpc

import (
	"net"

	"github.com/xichen2020/eventdb/generated/proto/servicepb"

	xserver "github.com/m3db/m3x/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type server struct {
	opts    *Options
	address string
	server  *grpc.Server
}

// NewServer creates a new http server.
func NewServer(
	address string,
	svc servicepb.EventdbServer,
	opts *Options,
) xserver.Server {
	if opts == nil {
		opts = NewOptions()
	}

	grpcServer := grpc.NewServer(
		grpc.ReadBufferSize(opts.ReadBufferSize()),
		grpc.MaxRecvMsgSize(opts.MaxRecvMsgSize()),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: opts.KeepAlivePeriod(),
		}),
	)
	servicepb.RegisterEventdbServer(grpcServer, svc)
	return &server{
		opts:    opts,
		address: address,
		server:  grpcServer,
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
	// `Stop` also closes the listeners.
	s.server.Stop()
	s.opts.InstrumentOptions().Logger().Info("grpc server closed")
}
