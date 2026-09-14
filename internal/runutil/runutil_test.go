package runutil

import (
	"testing"
	"time"
)

func TestContextCanBeCancelled(t *testing.T) {
	ctx, cancel := Context()
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not cancelled")
	}
}

func TestConfigureLogging(t *testing.T) {
	ConfigureLogging("test-service")
}
