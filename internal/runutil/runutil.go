package runutil

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// Context returns a context cancelled by the normal container termination signals.
func Context() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func ConfigureLogging(service string) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{})).With("service", service))
}

func Fatal(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
}
