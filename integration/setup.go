package integration

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/xichen2020/eventdb/services/eventdb/serve"
	"github.com/xichen2020/eventdb/storage"

	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/uber-go/tally"
)

const (
	gracefulShutdownTimeout = 15 * time.Second
)

var (
	logger = log.NullLogger
)

type closer func()

// setup sets up the database, a http server, and a client from the given config and returns the
// client and a closer that should be called once the tests are complete.
func setup(t *testing.T, configFname string) (client, closer) {
	var cfg configuration
	if err := xconfig.LoadFile(&cfg, configFname, xconfig.Options{}); err != nil {
		t.Fatalf("error loading config file %s: %v\n", configFname, err)
	}

	iOpts := instrument.NewOptions().
		SetMetricsScope(tally.NoopScope).
		SetLogger(logger)

	namespaces, err := cfg.Database.NewNamespacesMetadata()
	if err != nil {
		t.Fatalf("error creating namespaces metadata: %v", err)
	}
	shardSet, err := cfg.Database.NewShardSet()
	if err != nil {
		t.Fatalf("error creating shard set: %v", err)
	}
	dbOpts, err := cfg.Database.NewOptions(iOpts)
	if err != nil {
		t.Fatalf("error creating database options: %v", err)
	}
	db := storage.NewDatabase(namespaces, shardSet, dbOpts)
	if err := db.Open(); err != nil {
		t.Fatalf("error opening database: %v", err)
	}

	doneCh := make(chan struct{})
	closedCh := make(chan struct{})
	go func() {
		if err := serve.Serve(
			cfg.HTTP.ListenAddress,
			cfg.HTTP.Handler.NewOptions(iOpts),
			cfg.HTTP.NewServerOptions(),
			db,
			logger,
			doneCh,
		); err != nil {
			logger.Fatalf("could not start serving traffic: %v", err)
		}
		close(closedCh)
	}()

	closer := func() {
		close(doneCh)

		select {
		case <-closedCh:
			t.Log("server closed clean")
		case <-time.After(gracefulShutdownTimeout):
			t.Logf("server closed due to %s timeout", gracefulShutdownTimeout.String())
		}

		db.Close()
		// TODO(wjang): delete the database files as well
	}

	client := newClient(cfg.HTTP.ListenAddress)

	for i := 0; i < 100; i++ {
		if client.serverIsHealthy() {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if !client.serverIsHealthy() {
		// closer() // TODO(wjang): closer() is panic-ing
		t.Fatal("server is not up")
	}

	data, err := ioutil.ReadFile(cfg.InputFname)
	if err != nil {
		t.Fatalf("cannot read %s: %v", cfg.InputFname, err)
	}

	if err := client.write(data); err != nil {
		print(err.Error())
		// closer() // TODO(wjang): closer() is panic-ing
		t.Fatal("failed write to server")
	}

	return client, closer
}
