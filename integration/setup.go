package integration

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xichen2020/eventdb/server/grpc"
	"github.com/xichen2020/eventdb/server/http"
	"github.com/xichen2020/eventdb/server/http/handlers"
	"github.com/xichen2020/eventdb/services/eventdb/serve"
	"github.com/xichen2020/eventdb/sharding"
	"github.com/xichen2020/eventdb/storage"

	"github.com/m3db/m3x/instrument"
	"github.com/stretchr/testify/require"
	validator "gopkg.in/validator.v2"
	yaml "gopkg.in/yaml.v2"
)

const (
	serverStateChangeTimeout = 5 * time.Second
)

var (
	errServerStartTimedOut = errors.New("server took too long to start")
)

// TODO(xichen): Add GRPC server testing logic.

type testServerSetup struct {
	httpAddr        string
	httpServiceOpts *handlers.Options
	httpServerOpts  *http.Options
	grpcAddr        string
	grpcServiceOpts *grpc.ServiceOptions
	grpcServerOpts  *grpc.Options

	db         storage.Database
	namespaces []storage.NamespaceMetadata
	shardSet   sharding.ShardSet
	dbOpts     *storage.Options

	// Signals.
	doneCh   chan struct{}
	closedCh chan struct{}
}

func newTestServerSetup(t *testing.T, cfg configuration) *testServerSetup {
	namespaces, err := cfg.Database.NewNamespacesMetadata()
	require.NoError(t, err)

	shardSet, err := cfg.Database.NewShardSet()
	require.NoError(t, err)

	dbOpts, err := cfg.Database.NewOptions(instrument.NewOptions())
	require.NoError(t, err)

	return &testServerSetup{
		httpAddr:        cfg.HTTP.ListenAddress,
		httpServiceOpts: cfg.HTTP.Service.NewOptions(dbOpts.InstrumentOptions()),
		httpServerOpts:  cfg.HTTP.NewServerOptions(dbOpts.InstrumentOptions()),
		grpcAddr:        cfg.GRPC.ListenAddress,
		grpcServiceOpts: cfg.GRPC.Service.NewOptions(dbOpts.InstrumentOptions()),
		grpcServerOpts:  cfg.GRPC.NewServerOptions(dbOpts.InstrumentOptions()),
		namespaces:      namespaces,
		shardSet:        shardSet,
		dbOpts:          dbOpts,

		doneCh:   make(chan struct{}),
		closedCh: make(chan struct{}),
	}
}

func (ts *testServerSetup) newHTTPClient() httpClient {
	return newHTTPClient(ts.httpAddr)
}

func (ts *testServerSetup) startServer() error {
	errCh := make(chan error, 1)

	ts.db = storage.NewDatabase(ts.namespaces, ts.shardSet, ts.dbOpts)
	if err := ts.db.Open(); err != nil {
		return err
	}

	go func() {
		// TODO (wjang): pass in 0.0.0.0:0 instead, have an automatically generated port and use it.
		if err := serve.Serve(
			ts.grpcAddr,
			ts.grpcServiceOpts,
			ts.grpcServerOpts,
			ts.httpAddr,
			ts.httpServiceOpts,
			ts.httpServerOpts,
			ts.db,
			ts.dbOpts.InstrumentOptions().Logger(),
			ts.doneCh,
		); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
		close(ts.closedCh)
	}()

	go func() {
		select {
		case errCh <- ts.waitUntilServerIsUp():
		default:
		}
	}()

	return <-errCh
}

func (ts *testServerSetup) waitUntilServerIsUp() error {
	c := ts.newHTTPClient()
	serverIsUp := func() bool { return c.serverIsHealthy() }
	if waitUntil(serverIsUp, serverStateChangeTimeout) {
		return nil
	}
	return errServerStartTimedOut
}

func (ts *testServerSetup) stopServer() error {
	if err := ts.db.Close(); err != nil {
		return err
	}
	close(ts.doneCh)

	// Remove data directory to prevent accumulation of data.
	if err := os.RemoveAll(filepath.Join(ts.db.Options().FilePathPrefix(), "data")); err != nil {
		return err
	}

	// Wait for graceful server shutdown.
	<-ts.closedCh
	return nil
}

func (ts *testServerSetup) close(t *testing.T) {
	// TODO(wjang): Delete the database files as well.
}

func loadConfig(t *testing.T, config string) configuration {
	var cfg configuration
	require.NoError(t, yaml.UnmarshalStrict([]byte(config), &cfg))
	require.NoError(t, validator.Validate(&cfg))
	return cfg
}
