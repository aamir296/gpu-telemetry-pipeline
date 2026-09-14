package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/runutil"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/storage"
)

func main() {
	runutil.ConfigureLogging("retention")
	maxAge := flag.Duration("max-age", 0, "delete telemetry older than this duration")
	batchSize := flag.Int("batch-size", 10000, "maximum rows deleted per transaction")
	flag.Parse()
	if value := os.Getenv("RETENTION_MAX_AGE"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			runutil.Fatal("invalid RETENTION_MAX_AGE", err)
		}
		*maxAge = parsed
	}
	if value := os.Getenv("RETENTION_BATCH_SIZE"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			runutil.Fatal("invalid RETENTION_BATCH_SIZE", err)
		}
		*batchSize = parsed
	}
	if *maxAge <= 0 || *batchSize < 1 {
		runutil.Fatal("invalid retention configuration", errors.New("max age and batch size must be positive"))
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		runutil.Fatal("invalid configuration", errors.New("DATABASE_URL is required"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	repository, err := storage.Open(ctx, databaseURL)
	if err != nil {
		runutil.Fatal("connect to database", err)
	}
	defer repository.Close()
	before := time.Now().UTC().Add(-*maxAge)
	var total int64
	for {
		deleted, err := repository.PruneBefore(ctx, before, *batchSize)
		if err != nil {
			runutil.Fatal("prune telemetry", err)
		}
		total += deleted
		if deleted < int64(*batchSize) {
			break
		}
	}
	slog.Info("retention completed", "deleted", total, "before", before)
}
