package main

import (
	"log/slog"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/clock"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/observability"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/runutil"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/streamer"
)

func main() {
	runutil.ConfigureLogging("streamer")
	cfg, err := config.StreamerFromEnv()
	if err != nil {
		runutil.Fatal("invalid configuration", err)
	}
	ctx, stop := runutil.Context()
	defer stop()
	runner := streamer.New(cfg, clock.System{})
	go func() {
		if err := observability.Serve(ctx, cfg.MetricsAddr, runner.MetricsHandler()); err != nil {
			slog.Error("metrics server stopped", "error", err)
			stop()
		}
	}()
	slog.Info("streamer starting", "dataset", cfg.DatasetID, "csv", cfg.CSVPath, "rows_per_second", cfg.RowsPerSecond)
	if err := runner.Run(ctx); err != nil {
		runutil.Fatal("streamer stopped", err)
	}
}
