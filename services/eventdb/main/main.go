package main

import (
	"flag"

	m3config "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/log"

	"github.com/xichen2020/eventdb/server"
	"github.com/xichen2020/eventdb/services/eventdb/config"
	"github.com/xichen2020/eventdb/storage"
)

var (
	configFilePath string
	logger         = log.SimpleLogger
)

func main() {
	// Parse command line args.
	flag.Parse()

	var cfg config.Config
	if err := m3config.LoadFile(&cfg, configFilePath, m3config.Options{}); err != nil {
		logger.Fatalf("logstore load config error: %v", err)
	}

	// Instantiate DB.
	db := storage.New(storage.NewOptions())

	// Spin up server.
	s := server.New(cfg.Server.Address, db, server.NewOptions())
	if err := s.ListenAndServe(); err != nil {
		logger.Fatalf("Failed to serve HTTP endpoints: %v", err)
	}
}

func init() {
	flag.StringVar(&configFilePath, "config", "eventdb.yaml", "path to the eventdb config file")
}
