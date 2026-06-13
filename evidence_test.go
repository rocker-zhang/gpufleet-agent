package agent

import "testing"

// stubReader returns devices out of UUID order to prove Collect sorts.
type stubReader struct{}

func (stubReader) Read() (Evidence, error) {
	return Evidence{
		Source: "stub",
		Devices: []DeviceMetrics{
			{UUID: "GPU-z", Node: "n", Model: "A10", WindowSeconds: 10},
			{UUID: "GPU-a", Node: "n", Model: "A10", WindowSeconds: 10},
		},
	}, nil
}

func TestCollectDeterministicOrder(t *testing.T) {
	ev, err := Collect(stubReader{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Devices[0].UUID != "GPU-a" || ev.Devices[1].UUID != "GPU-z" {
		t.Fatalf("devices not sorted by UUID: %v", ev.Devices)
	}
}

// TestMockReaderShape (the default-build mock-shape assertions) lives in
// evidence_mock_test.go (//go:build !gpu); the gpu build exercises the real
// DefaultReader path in evidence_gpu_test.go (skip-not-fail when NVML is
// unwired). DefaultReader's concrete type is build-specific, so the shape
// assertions cannot be shared across both builds.
