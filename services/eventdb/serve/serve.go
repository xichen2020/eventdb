package serve

import (
	"fmt"

	"github.com/xichen2020/eventdb/server/grpc"
	httpserver "github.com/xichen2020/eventdb/server/http"
	"github.com/xichen2020/eventdb/server/http/handlers"
	"github.com/xichen2020/eventdb/storage"

	"go.uber.org/zap"
)

// Serve starts serving HTTP traffic.
func Serve(
	grpcAddr string,
	grpcServiceOpts *grpc.ServiceOptions,
	grpcServerOpts *grpc.Options,
	httpAddr string,
	httpServiceOpts *handlers.Options,
	httpServerOpts *httpserver.Options,
	db storage.Database,
	logger *zap.SugaredLogger,
	doneCh chan struct{},
) error {
	grpcService := grpc.NewService(db, grpcServiceOpts)
	grpcServer := grpc.NewServer(grpcAddr, grpcService, grpcServerOpts)
	if err := grpcServer.ListenAndServe(); err != nil {
		return fmt.Errorf("could not start grpc server at %s: %v", grpcAddr, err)
	}
	defer grpcServer.Close()

	logger.Infof("grpc server: listening on %s", grpcAddr)

	httpService := handlers.NewService(db, httpServiceOpts)
	httpServer := httpserver.NewServer(httpAddr, httpService, httpServerOpts)
	if err := httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("could not start http server at %s: %v", httpAddr, err)
	}
	defer httpServer.Close()
	logger.Infof("http server: listening on %s", httpAddr)

	// Wait for exit signal.
	<-doneCh

	return nil
}
