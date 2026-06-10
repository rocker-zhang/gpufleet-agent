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

func TestMockReaderShape(t *testing.T) {
	ev, err := Collect(DefaultReader("test-node"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Source != "mock" {
		t.Fatalf("default (no-gpu) build should use mock source, got %q", ev.Source)
	}
	if len(ev.Devices) != 2 {
		t.Fatalf("want 2 mock devices, got %d", len(ev.Devices))
	}
	for _, d := range ev.Devices {
		if d.Node != "test-node" {
			t.Errorf("node not propagated: %q", d.Node)
		}
		if d.WindowSeconds <= 0 {
			t.Errorf("non-positive window for %s", d.UUID)
		}
	}
}
