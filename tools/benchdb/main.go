// This tool is used to benchmark the performance impact of different database designs.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/parser/json/value"
	"github.com/xichen2020/eventdb/sharding"
	"github.com/xichen2020/eventdb/storage"

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/log"
	"github.com/pborman/uuid"
)

var (
	inputFile        = flag.String("inputFile", "", "input file containing sample events")
	maxParseDepth    = flag.Int("maxParseDepth", 3, "maximum parse depth")
	excludeKeySuffix = flag.String("excludeKeySuffix", "", "excluding keys with given suffix")
	numShards        = flag.Int("numShards", 8, "number of shards")
	numWorkers       = flag.Int("numWorkers", 1, "number of workers processing events in parallel")
	cpuProfileFile   = flag.String("cpuProfileFile", "cpu.profile", "path to CPU profile")

	logger         = log.SimpleLogger
	eventNamespace = []byte("testNamespace")
)

func main() {
	flag.Parse()

	if len(*inputFile) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	events, totalBytes, err := readEvents(*inputFile)
	if err != nil {
		logger.Fatalf("error reading events from input file %s: %v", *inputFile, err)
	}
	logger.Infof("read %d events from %d bytes", len(events), totalBytes)

	db, err := createDatabase()
	if err != nil {
		logger.Fatalf("error creating database: %v", err)
	}
	if err = db.Open(); err != nil {
		logger.Fatalf("error opening database: %v", err)
	}
	defer db.Close()

	if len(*cpuProfileFile) > 0 {
		f, err := os.Create(*cpuProfileFile)
		if err != nil {
			logger.Fatalf("could not create CPU profile: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			logger.Fatalf("could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	var (
		wg    sync.WaitGroup
		idx   int32 = -1
		start       = time.Now()
	)
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processEvents(db, events, &idx)
		}()
	}
	wg.Wait()

	var (
		dur          = time.Since(start)
		durInSeconds = float64(dur.Nanoseconds()) / 1e9
	)
	logger.Infof("processed %d events in %v, throughput = %f events / s", len(events), dur, float64(len(events))/durInSeconds)
	logger.Infof("processed %d bytes in %v, throughput = %f bytes / s", totalBytes, dur, float64(totalBytes)/durInSeconds)
}

func readEvents(fname string) ([]event.Event, int, error) {
	f, err := os.Open(fname)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	var (
		parserOpts = parserOptions()
		reader     = bufio.NewReader(f)
		events     []event.Event
		totalBytes int
		parseTime  time.Duration
		readErr    error
	)
	for readErr == nil {
		var eventBytes []byte
		eventBytes, readErr = reader.ReadBytes('\n')
		size := len(eventBytes)
		if size == 0 {
			continue
		}
		if eventBytes[size-1] == '\n' {
			eventBytes = eventBytes[:size-1]
		}
		totalBytes += len(eventBytes)
		parseStart := time.Now()
		p := json.NewParser(parserOpts)
		v, err := p.ParseBytes(eventBytes)
		parseTime += time.Since(parseStart)
		if err != nil {
			return nil, 0, fmt.Errorf("error parsing %s: %v", eventBytes, err)
		}
		ev := event.Event{
			ID:        []byte(uuid.NewUUID().String()),
			TimeNanos: time.Now().UnixNano(),
			FieldIter: value.NewFieldIterator(v),
			RawData:   eventBytes,
		}
		events = append(events, ev)
	}

	logger.Infof("parsing %d events in %v, throughput = %f events / s", len(events), parseTime, float64(len(events))/float64(parseTime)*1e9)
	logger.Infof("parsing %d bytes in %v, throughput = %f bytes / s", totalBytes, parseTime, float64(totalBytes)/float64(parseTime)*1e9)
	return events, totalBytes, nil
}

func parserOptions() *json.Options {
	opts := json.NewOptions().SetMaxDepth(*maxParseDepth)
	if len(*excludeKeySuffix) > 0 {
		filterFn := func(key string) bool { return strings.HasSuffix(key, *excludeKeySuffix) }
		opts = opts.SetObjectKeyFilterFn(filterFn)
	}
	return opts
}

func createDatabase() (storage.Database, error) {
	namespaces := [][]byte{eventNamespace}
	shardIDs := make([]uint32, 0, *numShards)
	for i := 0; i < *numShards; i++ {
		shardIDs = append(shardIDs, uint32(i))
	}
	shards := sharding.NewShards(shardIDs, shard.Available)
	hashFn := sharding.DefaultHashFn(*numShards)
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	if err != nil {
		return nil, fmt.Errorf("error creating shard set: %v", err)
	}
	return storage.NewDatabase(namespaces, shardSet, nil), nil
}

func processEvents(db storage.Database, events []event.Event, currIdx *int32) {
	for {
		newIdx := int(atomic.AddInt32(currIdx, 1))
		if newIdx >= len(events) {
			return
		}
		ev := events[newIdx]
		if err := db.Write(eventNamespace, ev); err != nil {
			logger.Errorf("error writing event %s: %v", ev.RawData, err)
		}
	}
}
