package model

import "testing"

func TestGPUKey(t *testing.T) {
	if got := (Telemetry{UUID: "GPU-1", Hostname: "n", GPUID: "0"}).GPUKey(); got != "GPU-1" {
		t.Fatal(got)
	}
	if got := (Telemetry{Hostname: "n", GPUID: "0"}).GPUKey(); got != "n/0" {
		t.Fatal(got)
	}
	if got := (Telemetry{Hostname: "n"}).GPUKey(); got != "" {
		t.Fatal(got)
	}
}
