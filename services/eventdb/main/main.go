package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/m3db/m3cluster/shard"
	m3config "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/log"

	"github.com/xichen2020/eventdb/server/http"
	"github.com/xichen2020/eventdb/server/http/handlers"
	"github.com/xichen2020/eventdb/services/eventdb/config"
	"github.com/xichen2020/eventdb/sharding"
	"github.com/xichen2020/eventdb/storage"
)

var (
	configFilePath string
	logger         = log.SimpleLogger
)

func main() {
	// Parse command line args.
	flag.Parse()

	logger.Info("loading config...")
	var cfg config.Config
	if err := m3config.LoadFile(&cfg, configFilePath); err != nil {
		logger.Fatalf("logstore load config error: %v", err)
	}

	// Instantiate DB.
	logger.Info("instantiating database...")
	shardIDs := make([]uint32, 0, cfg.Database.NumShards)
	for i := 0; i < cfg.Database.NumShards; i++ {
		shardIDs = append(shardIDs, uint32(i))
	}
	shards := sharding.NewShards(shardIDs, shard.Available)
	hashFn := sharding.DefaultHashFn(cfg.Database.NumShards)
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	if err != nil {
		logger.Fatalf("error initializing shards: %v", err)
	}
	db := storage.NewDatabase(cfg.Database.Namespaces, shardSet, cfg.Database.NewDatabaseOptions())
	if err := db.Open(); err != nil {
		logger.Fatalf("error opening db: %v", err)
	}
	defer db.Close()

	// Instantiate service and run server.
	logger.Info("instantiating and running server...")
	svc := handlers.NewService(db, nil)
	server := http.NewServer(cfg.Server.Address, svc, cfg.Server.NewServerOptions())
	if err := server.ListenAndServe(); err != nil {
		logger.Fatalf("error serving: %v", err)
	}
	defer server.Close()
	logger.Infof("server is running @ %s", cfg.Server.Address)

	logger.Warnf("interrupt: %v", interrupt())
}

func interrupt() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}

func init() {
	flag.StringVar(&configFilePath, "config", "eventdb.yaml", "path to the eventdb config file")
}
