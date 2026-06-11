//go:build !gpu

package agent

import (
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fixedNow gives every test a deterministic, replayable clock.
func fixedNow() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

func TestNormalizeMergesSourcesAndResolvesJobs(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	obs := collectAll(t, DefaultCollectors("test-node"), now, win)

	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}

	// Real generated proto type, not a mirror.
	if _, ok := any(w.Pack).(*gpufleetv1.EvidencePack); !ok {
		t.Fatalf("Pack is not the generated gpufleet.v1.EvidencePack")
	}
	if w.Pack.ContractVersion != "v1" {
		t.Fatalf("contract_version = %q, want v1", w.Pack.ContractVersion)
	}
	if len(w.Pack.Mappings) != 3 {
		t.Fatalf("want 3 device mappings, got %d", len(w.Pack.Mappings))
	}

	// All three SignalSources contributed and are recorded for provenance.
	wantSrc := map[gpufleetv1.SignalSource]bool{
		gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM:       true,
		gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS: true,
		gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID:  true,
	}
	for _, s := range w.Sources {
		delete(wantSrc, s)
	}
	if len(wantSrc) != 0 {
		t.Fatalf("missing sources in window: %v", wantSrc)
	}

	// semantics.ResolveMapping grouped two jobs: job-train-a (2 devices) +
	// job-idle-b (1 device).
	if len(w.Jobs) != 2 {
		t.Fatalf("want 2 resolved jobs, got %d: %+v", len(w.Jobs), w.Jobs)
	}
	if w.Jobs[0].Job.ID != "job-idle-b" || w.Jobs[1].Job.ID != "job-train-a" {
		t.Fatalf("jobs not sorted/resolved as expected: %+v", w.Jobs)
	}

	// Cross-source field completion: DCGM omitted GPU-mock-0003's peak; Prometheus
	// supplied it, so MFU IS computable for 0003 (it is in Samples).
	if _, ok := w.Samples["GPU-mock-0003"]; !ok {
		t.Fatalf("GPU-mock-0003 should be computable after cross-source peak completion")
	}
}

func TestNormalizeDegradesMissingFieldNeverFabricates(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	// Only DCGM, which omits the peak for GPU-mock-0003 AND supplies no
	// device->job mapping and no cost. The normalizer must DEGRADE, not invent.
	obs := collectAll(t, []Collector{MockDCGMCollector{Node: "n"}}, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}

	// GPU-mock-0003 has no peak from any source ⇒ NOT in Samples (MFU not faked).
	if _, ok := w.Samples["GPU-mock-0003"]; ok {
		t.Fatalf("GPU-mock-0003 MFU must be degraded (no peak), not fabricated")
	}
	// No Prometheus ⇒ no mappings ⇒ every device degraded on job_id, and every
	// device degraded on cost.
	var sawJobDegrade, sawMFUDegrade, sawCostDegrade bool
	for _, dm := range w.Degraded {
		switch dm.Field {
		case "job_id":
			sawJobDegrade = true
		case "mfu":
			sawMFUDegrade = true
		case "cost":
			sawCostDegrade = true
		}
	}
	if !sawJobDegrade {
		t.Errorf("expected job_id degradation when no scheduler/Prometheus source")
	}
	if !sawMFUDegrade {
		t.Errorf("expected mfu degradation for the peak-less device")
	}
	if !sawCostDegrade {
		t.Errorf("expected cost degradation when no $/hour source")
	}
	// With no mapping, no jobs resolve.
	if len(w.Jobs) != 0 {
		t.Fatalf("want 0 jobs with no mappings, got %d", len(w.Jobs))
	}
}

func TestNormalizeXidEventPassthrough(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	xid := []*gpufleetv1.XidEvent{{
		Ts: timestamppb.New(now), DeviceUuid: "GPU-mock-0002", Xid: 79,
		RawMessage: "NVRM: Xid (PCI:0000:65:00): 79, GPU has fallen off the bus",
		Severity:   "err",
	}}
	cols := []Collector{
		MockDCGMCollector{Node: "n"},
		MockPrometheusCollector{Node: "n"},
		MockDmesgCollector{Node: "n", Inject: xid},
	}
	obs := collectAll(t, cols, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if len(w.Pack.XidEvents) != 1 || w.Pack.XidEvents[0].Xid != 79 {
		t.Fatalf("XID event not passed through verbatim: %+v", w.Pack.XidEvents)
	}
}

func collectAll(t *testing.T, cols []Collector, now time.Time, win time.Duration) []Observation {
	t.Helper()
	var obs []Observation
	for _, c := range cols {
		o, err := c.Collect(now, win)
		if err != nil {
			t.Fatalf("collect %v: %v", c.Source(), err)
		}
		obs = append(obs, o)
	}
	return obs
}
