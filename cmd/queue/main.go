package main

import (
	"log/slog"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/server"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/observability"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/runutil"
)

func main() {
	runutil.ConfigureLogging("queue")
	cfg, err := config.QueueFromEnv()
	if err != nil {
		runutil.Fatal("invalid configuration", err)
	}
	broker, err := server.New(cfg.ListenAddr, cfg.DataDir, cfg.Partitions, cfg.SegmentBytes, cfg.MaxPendingBytes, cfg.MaxBatchRecords, cfg.MaxRecordBytes, cfg.MemberTTL, cfg.CycleInterval)
	if err != nil {
		runutil.Fatal("initialize queue", err)
	}
	ctx, stop := runutil.Context()
	defer stop()
	go func() {
		if err := observability.Serve(ctx, cfg.MetricsAddr, broker.MetricsHandler()); err != nil {
			slog.Error("metrics server stopped", "error", err)
			stop()
		}
	}()
	slog.Info("queue starting", "address", cfg.ListenAddr, "partitions", cfg.Partitions, "data_dir", cfg.DataDir)
	if err := broker.Serve(ctx); err != nil {
		runutil.Fatal("queue stopped", err)
	}
}
