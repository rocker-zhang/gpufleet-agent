//go:build gpu

package agent

import (
	"errors"
	"testing"
)

// TestDefaultReaderGPU exercises the gpu build's real DefaultReader (the
// read-only NVML-backed reader). On a NO-NVML environment — CI, an arm box, any
// dev machine without the lab NVML wiring — the reader returns errNVMLNotWired;
// we t.Skip with a clear reason instead of failing, so `go test -tags gpu ./...`
// is GREEN GPU-less (RULES §H, TASK-0034 DoD). On a real NVML lab node the
// reader returns evidence and we validate the real path (deterministic UUID
// sort + node propagation), so the gpu suite is a true smoke test there.
func TestDefaultReaderGPU(t *testing.T) {
	ev, err := Collect(DefaultReader("test-node"))
	if errors.Is(err, errNVMLNotWired) {
		t.Skipf("skipping: NVML reader not wired in this environment (lab-only); "+
			"run on a real NVML node to exercise the read-only NVML path: %v", err)
	}
	if err != nil {
		t.Fatalf("unexpected error from gpu DefaultReader: %v", err)
	}
	// Real NVML path: validate the build-agnostic invariants Collect guarantees.
	for i := 1; i < len(ev.Devices); i++ {
		if ev.Devices[i-1].UUID > ev.Devices[i].UUID {
			t.Fatalf("devices not sorted by UUID: %v", ev.Devices)
		}
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
