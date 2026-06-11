//go:build !gpu

package agent

import (
	"testing"
	"time"
)

// TestCostWedgeHealthyZeroIdlePositive proves the standalone cost wedge: a
// healthy (high-MFU) device wastes $0, an idle (low-MFU) device wastes > $0 —
// with NO fault, RCA, or gate involved (pure semantics standalone path).
func TestCostWedgeHealthyZeroIdlePositive(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	obs := collectAll(t, DefaultCollectors("test-node"), now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	rep, err := w.CostWedge(DefaultCostPolicy())
	if err != nil {
		t.Fatalf("CostWedge: %v", err)
	}

	byUUID := map[string]float64{}
	priced := map[string]bool{}
	for _, d := range rep.Devices {
		byUUID[d.Device.Device.UUID] = d.WastedUSD
		priced[d.Device.Device.UUID] = d.Impact.Computed
	}

	healthy, ok := byUUID["GPU-mock-0001"]
	if !ok {
		t.Fatalf("healthy device missing from cost report")
	}
	idle, ok := byUUID["GPU-mock-0002"]
	if !ok {
		t.Fatalf("idle device missing from cost report")
	}

	// The healthy device runs at ~70% MFU; wasted is the idle-fraction spend, so
	// it is small but the IDLE device must waste strictly MORE. The acceptance
	// criterion is healthy≈$0-ish vs idle>0; assert idle is clearly positive and
	// much larger than healthy.
	if !priced["GPU-mock-0001"] || !priced["GPU-mock-0002"] {
		t.Fatalf("both devices should be priced (Prometheus supplied $/hr)")
	}
	if idle <= 0 {
		t.Fatalf("idle device must waste > $0, got %.6f", idle)
	}
	if idle <= healthy {
		t.Fatalf("idle waste (%.6f) must exceed healthy waste (%.6f)", idle, healthy)
	}

	// Tighten the "healthy = $0" acceptance: at 100% MFU the wedge is exactly $0.
	// Build a fully-utilized device window and assert zero.
	zeroW := buildFullUtilWindow(t, now, win)
	zr, err := zeroW.CostWedge(DefaultCostPolicy())
	if err != nil {
		t.Fatalf("CostWedge(full-util): %v", err)
	}
	if len(zr.Devices) != 1 || zr.Devices[0].WastedUSD != 0 {
		t.Fatalf("a 100%%-MFU healthy device must waste exactly $0, got %+v", zr.Devices)
	}

	// Idle device meets the deterministic LOW_UTILIZATION rule (low MFU AND low
	// tensor-active); healthy device does not.
	for _, d := range rep.Devices {
		if d.Device.Device.UUID == "GPU-mock-0002" && !d.LowUtilization {
			t.Errorf("idle device should meet LOW_UTILIZATION rule")
		}
		if d.Device.Device.UUID == "GPU-mock-0001" && d.LowUtilization {
			t.Errorf("healthy device should NOT meet LOW_UTILIZATION rule")
		}
	}
}

// buildFullUtilWindow normalizes a single 100%-MFU, priced device.
func buildFullUtilWindow(t *testing.T, now time.Time, win time.Duration) *SignalWindow {
	t.Helper()
	ws := win.Seconds()
	obs := []Observation{{
		Source:   MockPrometheusCollector{}.Source(),
		Mappings: nil,
		DeviceWindows: []DeviceWindow{{
			UUID: "GPU-full-0001", Node: "n", Model: "A10", WindowSeconds: ws,
			AchievedFLOPs: peakA10FLOPS * ws, AchievedFLOPsKnown: true,
			TensorActiveSecs: ws, TensorActiveKnown: true,
			PeakFLOPS: peakA10FLOPS, PeakFLOPSKnown: true,
			CostPerHour: 1.10, CostKnown: true,
		}},
	}}
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize(full-util): %v", err)
	}
	return w
}
