//go:build !gpu

package agent

import "testing"

// TestMockReaderShape asserts the default (no-GPU) build's DefaultReader is the
// deterministic mock source. These assertions are mock-specific (source name +
// fixed device count), so they are scoped to the !gpu build; the gpu build's
// real DefaultReader is exercised (skip-not-fail) in evidence_gpu_test.go.
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
