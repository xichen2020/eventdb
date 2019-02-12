package integration

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3x/instrument"
	"github.com/xichen2020/eventdb/server/http"
	"github.com/xichen2020/eventdb/server/http/handlers"
	"github.com/xichen2020/eventdb/services/eventdb/serve"
	"github.com/xichen2020/eventdb/sharding"
	"github.com/xichen2020/eventdb/storage"

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

type testServerSetup struct {
	addr        string
	db          storage.Database
	namespaces  []storage.NamespaceMetadata
	shardSet    sharding.ShardSet
	dbOpts      *storage.Options
	handlerOpts *handlers.Options
	serverOpts  *http.Options

	// Signals.
	doneCh   chan struct{}
	closedCh chan struct{}
}

func newTestServerSetup(t *testing.T, config string) *testServerSetup {
	cfg := loadConfig(t, config)

	namespaces, err := cfg.Database.NewNamespacesMetadata()
	require.NoError(t, err)

	shardSet, err := cfg.Database.NewShardSet()
	require.NoError(t, err)

	dbOpts, err := cfg.Database.NewOptions(instrument.NewOptions())
	require.NoError(t, err)

	return &testServerSetup{
		addr:        cfg.HTTP.ListenAddress,
		namespaces:  namespaces,
		shardSet:    shardSet,
		dbOpts:      dbOpts,
		handlerOpts: cfg.HTTP.Handler.NewOptions(dbOpts.InstrumentOptions()),
		serverOpts:  cfg.HTTP.NewServerOptions(dbOpts.InstrumentOptions()),
		doneCh:      make(chan struct{}),
		closedCh:    make(chan struct{}),
	}
}

func (ts *testServerSetup) newClient() client {
	return newClient(ts.addr)
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
			ts.addr,
			ts.handlerOpts,
			ts.serverOpts,
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
	c := ts.newClient()
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
