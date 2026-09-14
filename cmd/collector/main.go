package main

import (
	"log/slog"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/collector"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/observability"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/runutil"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/storage"
)

func main() {
	runutil.ConfigureLogging("collector")
	cfg, err := config.CollectorFromEnv()
	if err != nil {
		runutil.Fatal("invalid configuration", err)
	}
	ctx, stop := runutil.Context()
	defer stop()
	repo, err := storage.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		runutil.Fatal("connect to database", err)
	}
	defer repo.Close()
	slog.Info("collector starting", "queue", cfg.QueueAddr)
	runner := collector.New(cfg, repo)
	go func() {
		if err := observability.Serve(ctx, cfg.MetricsAddr, runner.MetricsHandler()); err != nil {
			slog.Error("metrics server stopped", "error", err)
			stop()
		}
	}()
	if err := runner.Run(ctx); err != nil {
		runutil.Fatal("collector stopped", err)
	}
}
