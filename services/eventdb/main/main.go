package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xichen2020/eventdb/services/eventdb/config"
	"github.com/xichen2020/eventdb/services/eventdb/serve"
	"github.com/xichen2020/eventdb/storage"

	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
)

const (
	gracefulShutdownTimeout = 15 * time.Second
)

var (
	configFile = flag.String("f", "config.yaml", "configuration file")
)

func main() {
	// Parse command line args.
	flag.Parse()

	if len(*configFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, *configFile, xconfig.Options{}); err != nil {
		fmt.Printf("error loading config file %s: %v\n", *configFile, err)
		os.Exit(1)
	}

	// Create logger and metrics scope.
	logger, err := cfg.Logging.BuildLogger()
	if err != nil {
		fmt.Printf("error creating logger: %v\n", err)
		os.Exit(1)
	}
	scope, closer, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatalf("error creating metrics root scope: %v", err)
	}
	defer closer.Close()

	iOpts := instrument.NewOptions().
		SetMetricsSamplingRate(cfg.Metrics.SampleRate())

	// Instantiate database.
	logger.Info("creating database...")
	namespaces, err := cfg.Database.NewNamespacesMetadata()
	if err != nil {
		logger.Fatalf("error creating namespaces metadata: %v", err)
	}
	shardSet, err := cfg.Database.NewShardSet()
	if err != nil {
		logger.Fatalf("error creating shard set: %v", err)
	}
	dbOpts, err := cfg.Database.NewOptions(iOpts.SetMetricsScope(scope.SubScope("database")))
	if err != nil {
		logger.Fatalf("error creating database options: %v", err)
	}
	db := storage.NewDatabase(namespaces, shardSet, dbOpts)
	if err := db.Open(); err != nil {
		logger.Fatalf("error opening database: %v", err)
	}
	defer db.Close()

	// Start up HTTP server.
	logger.Info("starting HTTP server...")
	handlerOpts := cfg.HTTP.Handler.NewOptions(iOpts.SetMetricsScope(scope.SubScope("http-handler")))
	serverOpts := cfg.HTTP.NewServerOptions()
	doneCh := make(chan struct{})
	closedCh := make(chan struct{})
	go func() {
		if err := serve.Serve(
			cfg.HTTP.ListenAddress,
			handlerOpts,
			serverOpts,
			db,
			logger,
			doneCh,
		); err != nil {
			logger.Fatalf("could not start serving traffic: %v", err)
		}
		logger.Debug("server closed")
		close(closedCh)
	}()

	// Handle interrupts.
	logger.Warnf("interrupt: %v", interrupt())

	close(doneCh)

	select {
	case <-closedCh:
		logger.Info("server closed clean")
	case <-time.After(gracefulShutdownTimeout):
		logger.Infof("server closed due to %s timeout", gracefulShutdownTimeout.String())
	}
}

func interrupt() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
