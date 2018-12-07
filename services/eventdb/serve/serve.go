package serve

import (
	"fmt"

	httpserver "github.com/xichen2020/eventdb/server/http"
	"github.com/xichen2020/eventdb/server/http/handlers"
	"github.com/xichen2020/eventdb/storage"

	"github.com/m3db/m3x/log"
)

// Serve starts serving HTTP traffic.
func Serve(
	addr string,
	handlerOpts *handlers.Options,
	serverOpts *httpserver.Options,
	db storage.Database,
	logger log.Logger,
	doneCh chan struct{},
) error {
	service := handlers.NewService(db, handlerOpts)
	httpServer := httpserver.NewServer(addr, service, serverOpts)
	if err := httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("could not start http server at %s: %v", addr, err)
	}
	defer httpServer.Close()
	logger.Infof("http server: listening on %s", addr)

	// Wait for exit signal.
	<-doneCh

	return nil
}
