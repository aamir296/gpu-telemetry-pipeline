package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/runutil"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/storage"
)

func main() {
	runutil.ConfigureLogging("migrate")
	timeout := flag.Duration("timeout", 90*time.Second, "maximum time to wait for PostgreSQL")
	flag.Parse()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		runutil.Fatal("invalid configuration", errors.New("DATABASE_URL is required"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	delay := 250 * time.Millisecond
	for {
		err := storage.Migrate(ctx, databaseURL)
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			runutil.Fatal("migration timed out", fmt.Errorf("%w: %v", ctx.Err(), err))
		case <-time.After(delay):
		}
		if delay < 5*time.Second {
			delay *= 2
		}
	}
}
