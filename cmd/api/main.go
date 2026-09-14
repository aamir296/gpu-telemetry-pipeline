package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	telemetryapi "github.com/aamir296/gpu-telemetry-pipeline/internal/api"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/runutil"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/storage"
)

func main() {
	runutil.ConfigureLogging("api")
	cfg, err := config.APIFromEnv()
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
	handler, _ := telemetryapi.New(repo, cfg)
	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	slog.Info("API starting", "address", cfg.ListenAddr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		runutil.Fatal("API stopped", err)
	}
}
