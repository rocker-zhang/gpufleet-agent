//go:build !gpu

package agent

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// TestKmsgMultiLineContinuation proves the kmsg continuation-line parser
// (TASK-0033 #4): a real /dev/kmsg record can span a header line plus
// space/tab-prefixed continuation lines. The Xid number, the trailing message,
// and the GPU UUID may all land on continuation lines; the parser must fold them
// into the header record so the event is still parsed and the raw record is
// preserved. Tested with NO hardware via the fixture.
func TestKmsgMultiLineContinuation(t *testing.T) {
	now := fixedNow()
	kmsg, err := os.ReadFile("testdata/kmsg_xid_multiline.txt")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	events := parseKmsgXID(strings.NewReader(string(kmsg)), now)
	if len(events) != 2 {
		t.Fatalf("want 2 XID events from multi-line records, got %d: %+v", len(events), events)
	}

	// First record: "79" is on the header line; the message + DEVICE continuation
	// lines fold in. The fallen-off-the-bus text (a continuation line) must be
	// preserved in the raw record.
	if events[0].Xid != 79 {
		t.Fatalf("first XID should be 79, got %d", events[0].Xid)
	}
	if !strings.Contains(events[0].RawMessage, "fallen off the bus") {
		t.Fatalf("continuation message line not folded into record: %q", events[0].RawMessage)
	}

	// Second record: the Xid NUMBER (48) AND the GPU UUID are BOTH on continuation
	// lines after the header `Xid (PCI:...):`. Without continuation folding the
	// header line alone has no number and would be dropped — the test proves it is
	// parsed.
	if events[1].Xid != 48 {
		t.Fatalf("second XID (48) lives on a continuation line; got %d — continuation not folded", events[1].Xid)
	}
	if !strings.HasPrefix(events[1].DeviceUuid, "GPU-aabbccdd") {
		t.Fatalf("GPU UUID from a continuation line not parsed: %q", events[1].DeviceUuid)
	}
	if !strings.Contains(events[1].RawMessage, "Double Bit ECC Error") {
		t.Fatalf("continuation ECC text not preserved: %q", events[1].RawMessage)
	}
}

// TestKmsgSingleLineStillParses guards against a regression: the continuation
// folding must not break the ordinary single-line-record case the existing
// fixture covers.
func TestKmsgSingleLineStillParses(t *testing.T) {
	now := fixedNow()
	kmsg, _ := os.ReadFile("testdata/kmsg_xid.txt")
	events := parseKmsgXID(strings.NewReader(string(kmsg)), now)
	if len(events) != 2 {
		t.Fatalf("single-line fixture regressed: want 2 events, got %d", len(events))
	}
	if events[0].Xid != 79 || events[1].Xid != 48 {
		t.Fatalf("single-line XIDs wrong: %d, %d", events[0].Xid, events[1].Xid)
	}
}

// stubCollector is a minimal Collector for chain/daemon tests: it returns a fixed
// observation (with one device window so the chain treats it as "has data") or a
// fixed error.
type stubCollector struct {
	src      gpufleetv1.SignalSource
	err      error
	hasData  bool
	uuid     string
	collects int
}

func (s *stubCollector) Source() gpufleetv1.SignalSource { return s.src }

func (s *stubCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	s.collects++
	if s.err != nil {
		return Observation{}, s.err
	}
	obs := Observation{Source: s.src, Provenance: map[string]string{}}
	if s.hasData {
		obs.DeviceWindows = []DeviceWindow{{
			UUID: s.uuid, WindowSeconds: window.Seconds(),
			AchievedFLOPs: 1e14, AchievedFLOPsKnown: true,
			PeakFLOPS: 1.25e14, PeakFLOPSKnown: true,
			TensorActiveSecs: 30, TensorActiveKnown: true,
			CostPerHour: 1.1, CostKnown: true,
		}}
	}
	return obs, nil
}

// TestMetricsChainSourceReflectsFallback proves TASK-0033 #2: after the chain
// falls back to DCGM, Source() returns DCGM (the ACTUALLY-selected source),
// matching the emitted Observation.Source — not the declared Prometheus
// preference. This keeps independence-by-source honest before the M3 gate.
func TestMetricsChainSourceReflectsFallback(t *testing.T) {
	// Primary (Prometheus) returns no data ⇒ fallback to DCGM engages.
	prom := &stubCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS, hasData: false}
	dcgm := &stubCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM, hasData: true, uuid: "GPU-x"}
	chain := NewMetricsChain(prom, dcgm)

	// Before any Collect, Source() is the declared preference (Prometheus).
	if chain.Source() != gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS {
		t.Fatalf("pre-Collect Source() should be the declared Prometheus preference, got %v", chain.Source())
	}

	obs, err := chain.Collect(fixedNow(), time.Minute)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if obs.Source != gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM {
		t.Fatalf("fallback observation must carry DCGM source, got %v", obs.Source)
	}
	// THE FIX: Source() now reflects the actually-selected source after fallback.
	if chain.Source() != gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM {
		t.Fatalf("after fallback, Source() must return the ACTUALLY-selected DCGM, got %v", chain.Source())
	}
	// And it must agree with Observation.Source (the independence-by-source contract).
	if chain.Source() != obs.Source {
		t.Fatalf("Source()=%v must match Observation.Source=%v after fallback", chain.Source(), obs.Source)
	}
}

// TestMetricsChainSourcePrimaryWhenPresent proves Source() reports the primary
// source when the primary actually answered (no spurious fallback tag).
func TestMetricsChainSourcePrimaryWhenPresent(t *testing.T) {
	prom := &stubCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS, hasData: true, uuid: "GPU-y"}
	dcgm := &stubCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM, hasData: true, uuid: "GPU-y"}
	chain := NewMetricsChain(prom, dcgm)
	obs, err := chain.Collect(fixedNow(), time.Minute)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if obs.Source != gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS {
		t.Fatalf("primary observation must carry Prometheus source, got %v", obs.Source)
	}
	if chain.Source() != gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS {
		t.Fatalf("Source() must report Prometheus when primary answered, got %v", chain.Source())
	}
	if dcgm.collects != 0 {
		t.Fatalf("fallback must not be consulted when primary has data")
	}
}

// TestMetricsChainSourceSharedAcrossValueCopy proves the selection is visible
// even when the chain is stored BY VALUE in a []Collector (as DefaultCollectors
// does): the shared pointer cell makes the copy observe the fallback selection.
func TestMetricsChainSourceSharedAcrossValueCopy(t *testing.T) {
	prom := &stubCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS, hasData: false}
	dcgm := &stubCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM, hasData: true, uuid: "GPU-z"}
	collectors := []Collector{NewMetricsChain(prom, dcgm)} // stored by value

	if _, err := collectors[0].Collect(fixedNow(), time.Minute); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if collectors[0].Source() != gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM {
		t.Fatalf("value-stored chain must reflect fallback selection via shared cell, got %v", collectors[0].Source())
	}
}

// softErrCollector returns ErrProfilingCapped (a soft/degradable error).
type softErrCollector struct{ src gpufleetv1.SignalSource }

func (s softErrCollector) Source() gpufleetv1.SignalSource { return s.src }
func (s softErrCollector) Collect(time.Time, time.Duration) (Observation, error) {
	return Observation{}, ErrProfilingCapped
}

// TestDaemonRefreshSoftErrorKeepsOtherSources proves TASK-0033 #3: a soft
// (ErrProfilingCapped) failure on ONE collector must NOT drop the whole cycle's
// Normalize — the other sources' data is still normalized and published that
// round. The previous "first error returns" behavior would blank the window.
func TestDaemonRefreshSoftErrorKeepsOtherSources(t *testing.T) {
	// One source caps (soft); the others (mock DCGM/Prom/dmesg) succeed.
	collectors := []Collector{
		softErrCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM},
		MockDCGMCollector{Node: "n"},
		MockPrometheusCollector{Node: "n"},
		MockDmesgCollector{Node: "n"},
	}
	d := NewDaemon(DaemonConfig{
		AgentID:    "soft-degrade",
		Window:     time.Minute,
		Collectors: collectors,
		Now:        fixedNow,
	})

	// Refresh must SUCCEED (publish the partial window), not error out the whole
	// cycle on the soft cap.
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("soft cap must not fail the whole refresh: %v", err)
	}
	st := d.Snapshot()
	if st == nil || st.Window == nil || st.Window.Pack == nil {
		t.Fatalf("partial window must still be published after a soft error")
	}
	// The other sources' device data survived (the mock DCGM/Prom devices).
	if len(st.Window.Pack.Mappings) == 0 {
		t.Fatalf("other sources' data was dropped by a soft error (regression)")
	}
	// The soft error was counted (off-path: counted, not propagated as node impact).
	if d.Errs() == 0 {
		t.Fatalf("soft error should still be counted via Errs()")
	}
	// Partial-collection provenance is recorded.
	if st.Window.Pack.Provenance["agent.partial_collection"] != "true" {
		t.Fatalf("partial-collection provenance not stamped: %+v", st.Window.Pack.Provenance)
	}
	if st.Window.Pack.Provenance["agent.soft_capped_sources"] != "1" {
		t.Fatalf("soft-capped source count wrong: %q", st.Window.Pack.Provenance["agent.soft_capped_sources"])
	}
}

// hardErrCollector returns a NON-soft (hard) error — not ErrProfilingCapped — to
// model a single source genuinely failing (scrape refused, parse blew up, etc.).
type hardErrCollector struct{ src gpufleetv1.SignalSource }

func (h hardErrCollector) Source() gpufleetv1.SignalSource { return h.src }
func (h hardErrCollector) Collect(time.Time, time.Duration) (Observation, error) {
	return Observation{}, errors.New("agent: hard source failure (scrape refused)")
}

// TestDaemonRefreshHardSingleSourceKeepsOthers locks in the §A robustness
// (TASK-0033 #3): a HARD single-source error (NOT a soft ErrProfilingCapped one)
// must NOT blank the window — the OTHER sources' data is still normalized and
// published that cycle. Only an all-sources failure keeps the stale State. This
// is the regression guard for the old "first collector error returns" behavior.
func TestDaemonRefreshHardSingleSourceKeepsOthers(t *testing.T) {
	collectors := []Collector{
		hardErrCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID}, // hard-fails
		MockDCGMCollector{Node: "n"},
		MockPrometheusCollector{Node: "n"},
	}
	d := NewDaemon(DaemonConfig{
		AgentID:    "hard-degrade",
		Window:     time.Minute,
		Collectors: collectors,
		Now:        fixedNow,
	})

	// A hard single-source error must NOT fail the whole cycle: the other sources
	// still publish.
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("a hard SINGLE-source error must not fail the whole refresh: %v", err)
	}
	st := d.Snapshot()
	if st == nil || st.Window == nil || st.Window.Pack == nil {
		t.Fatalf("window must still be published when only ONE source hard-fails")
	}
	if len(st.Window.Pack.Mappings) == 0 {
		t.Fatalf("surviving sources' data was dropped by a hard single-source error (regression)")
	}
	// The hard error is counted (off-path), and provenance records the partial.
	if d.Errs() == 0 {
		t.Fatalf("hard error should still be counted via Errs()")
	}
	if st.Window.Pack.Provenance["agent.partial_collection"] != "true" {
		t.Fatalf("partial-collection provenance not stamped on hard single-source failure: %+v", st.Window.Pack.Provenance)
	}
	if st.Window.Pack.Provenance["agent.failed_sources"] != "1" {
		t.Fatalf("failed-source count wrong: %q", st.Window.Pack.Provenance["agent.failed_sources"])
	}
	// It is NOT counted as a soft cap (it was a hard error).
	if st.Window.Pack.Provenance["agent.soft_capped_sources"] != "0" {
		t.Fatalf("hard error must not be counted as soft-capped: %q", st.Window.Pack.Provenance["agent.soft_capped_sources"])
	}
}

// TestDaemonRefreshAllHardKeepsStale proves that when EVERY source hard-fails the
// previous (stale-but-valid) State is preserved, not blanked — the all-failure
// path, distinct from the single-source degrade above.
func TestDaemonRefreshAllHardKeepsStale(t *testing.T) {
	good := []Collector{MockDCGMCollector{Node: "n"}, MockPrometheusCollector{Node: "n"}}
	d := NewDaemon(DaemonConfig{AgentID: "all-hard", Window: time.Minute, Collectors: good, Now: fixedNow})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	first := d.Snapshot()
	if first == nil {
		t.Fatalf("first refresh must publish")
	}

	d.collectors = []Collector{
		hardErrCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM},
		hardErrCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS},
	}
	if err := d.Refresh(context.Background()); err == nil {
		t.Fatalf("all-sources-hard-failed refresh should surface the error")
	}
	if cur := d.Snapshot(); cur != first {
		t.Fatalf("all-hard-failed round must keep the previous State, not replace it")
	}
}

// TestDaemonRefreshAllSoftKeepsStale proves that when EVERY source fails this
// round, Refresh keeps the previous (stale-but-valid) State rather than
// publishing an empty window over a good one.
func TestDaemonRefreshAllSoftKeepsStale(t *testing.T) {
	good := []Collector{MockDCGMCollector{Node: "n"}, MockPrometheusCollector{Node: "n"}}
	d := NewDaemon(DaemonConfig{AgentID: "stale", Window: time.Minute, Collectors: good, Now: fixedNow})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	first := d.Snapshot()
	if first == nil {
		t.Fatalf("first refresh must publish")
	}

	// Swap in all-failing collectors and refresh again.
	d.collectors = []Collector{
		softErrCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM},
		softErrCollector{src: gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS},
	}
	err := d.Refresh(context.Background())
	if err == nil {
		t.Fatalf("all-sources-failed refresh should surface the error")
	}
	// The previous State is preserved (stale-but-valid), not blanked.
	cur := d.Snapshot()
	if cur != first {
		t.Fatalf("all-failed round must keep the previous State, not replace it")
	}
}
