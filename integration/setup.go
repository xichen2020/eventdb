package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/xichen2020/eventdb/services/eventdb/serve"
	"github.com/xichen2020/eventdb/storage"

	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	validator "gopkg.in/validator.v2"
	yaml "gopkg.in/yaml.v2"
)

const (
	gracefulShutdownTimeout = 15 * time.Second
)

var (
	logger = log.NullLogger
)

type testServerSetup struct {
	db   storage.Database
	opts instrument.Options
	cfg  configuration

	// Signals.
	doneCh   chan struct{}
	closedCh chan struct{}
}

func newTestServerSetup(t *testing.T, config string) *testServerSetup {
	cfg := loadConfig(t, config)

	iOpts := instrument.NewOptions().
		SetMetricsScope(tally.NoopScope).
		SetLogger(logger)

	namespaces, err := cfg.Database.NewNamespacesMetadata()
	require.NoError(t, err)

	shardSet, err := cfg.Database.NewShardSet()
	require.NoError(t, err)

	dbOpts, err := cfg.Database.NewOptions(iOpts)
	require.NoError(t, err)

	db := storage.NewDatabase(namespaces, shardSet, dbOpts)
	require.NoError(t, db.Open())

	return &testServerSetup{
		db:       db,
		opts:     iOpts,
		cfg:      cfg,
		doneCh:   make(chan struct{}),
		closedCh: make(chan struct{}),
	}
}

func (ts *testServerSetup) startServer() {
	go func() {
		// TODO (wjang): pass in 0.0.0.0:0 instead, have an automatically generated port and use it.
		if err := serve.Serve(
			ts.cfg.HTTP.ListenAddress,
			ts.cfg.HTTP.Handler.NewOptions(ts.opts),
			ts.cfg.HTTP.NewServerOptions(),
			ts.db,
			logger,
			ts.doneCh,
		); err != nil {
			logger.Fatalf("could not start serving traffic: %v", err)
		}
		close(ts.closedCh)
	}()
}

func (ts *testServerSetup) stopServer(t *testing.T) {
	close(ts.doneCh)

	select {
	case <-ts.closedCh:
		t.Log("server closed clean")
	case <-time.After(gracefulShutdownTimeout):
		t.Logf("server closed due to %v timeout", gracefulShutdownTimeout)
	}
}

func (ts *testServerSetup) stopDB(t *testing.T) {
	// TODO(wjang): Delete the database files as well.
	require.NoError(t, ts.db.Close())
}

func (ts *testServerSetup) close(t *testing.T) {
	ts.stopServer(t)
	ts.stopDB(t)
}

func (ts *testServerSetup) newClient() client {
	return newClient(ts.cfg.HTTP.ListenAddress)
}

func (ts *testServerSetup) waitUntil(timeout time.Duration, condition func() bool) error {
	start := time.Now()
	for !condition() {
		time.Sleep(time.Millisecond * 10)
		if dur := time.Now().Sub(start); dur >= timeout {
			return fmt.Errorf("timeout waiting for condition")
		}
	}
	return nil
}

func loadConfig(t *testing.T, config string) configuration {
	var cfg configuration
	require.NoError(t, yaml.UnmarshalStrict([]byte(config), &cfg))
	require.NoError(t, validator.Validate(&cfg))
	return cfg
}
