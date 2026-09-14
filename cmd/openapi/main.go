package main

import (
	"fmt"
	"os"

	telemetryapi "github.com/aamir296/gpu-telemetry-pipeline/internal/api"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
)

func main() {
	cfg, err := config.APIFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_, documented := telemetryapi.New(nil, cfg)
	spec, err := documented.OpenAPI().YAML()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_, _ = os.Stdout.Write(spec)
}
