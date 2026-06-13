//go:build !gpu

package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// dcgmFixtureServer serves the committed DCGM-exporter text fixture so a real
// DCGMExporterCollector scrapes a LIVE endpoint and produces a real window. The
// returned server can be Closed to simulate the exporter becoming unreachable.
func dcgmFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()
	payload, err := os.ReadFile("testdata/dcgm_exporter.prom")
	if err != nil {
		t.Fatalf("read dcgm fixture: %v", err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
}

// TestFreshnessFreshThenStaleOnExporterUnreachable is the TASK-0040 core DoD:
//
//  1. FRESH: a successful collection ⇒ small age, stale=false, the window is
//     served as live.
//  2. STALE: close the exporter httptest server mid-run so every subsequent
//     collection FAILS; advance the clock past the threshold. The daemon must NOT
//     crash, must KEEP the last-known window (last value retained), and must flag
//     it stale=true with a provenance reason — never blank, never fabricate.
func TestFreshnessFreshThenStaleOnExporterUnreachable(t *testing.T) {
	srv := dcgmFixtureServer(t)
	defer srv.Close()

	clk := &manualClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}

	dcgm := &DCGMExporterCollector{ScrapeURL: srv.URL, Node: "lab-node"}
	d := NewDaemon(DaemonConfig{
		AgentID:        "freshness",
		Window:         time.Minute,
		StalenessAfter: 10 * time.Second,
		Collectors:     []Collector{dcgm},
		Policy:         DefaultCostPolicy(),
		Now:            clk.now,
	})

	// 1) FRESH — first collection succeeds.
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("initial Refresh against live exporter failed: %v", err)
	}
	st := d.Snapshot()
	if st == nil || st.Window == nil || st.Window.Pack == nil {
		t.Fatalf("expected a published window after a successful collection")
	}
	// The fixture carries tensor-active but no peak/cost, so cost wedges may be
	// unpriced; what matters for freshness is that a real WINDOW (device mappings)
	// was published and is retained across later failures.
	devCount := len(st.Window.Pack.Mappings)
	if devCount == 0 {
		t.Fatalf("expected device mappings in the published window from the live exporter")
	}

	fr := d.Freshness()
	if !fr.HasData {
		t.Fatalf("freshness should report HasData after a successful collection")
	}
	if fr.Stale {
		t.Fatalf("fresh data must not be stale: %+v", fr)
	}
	if fr.Age != 0 { // same instant: collected_at == now
		t.Fatalf("fresh age should be ~0 at collection instant, got %v", fr.Age)
	}

	// A small advance, still under the threshold, stays fresh.
	clk.advance(3 * time.Second)
	if fr := d.Freshness(); fr.Stale {
		t.Fatalf("age %v < threshold must stay fresh, got stale: %+v", fr.Age, fr)
	} else if fr.Age != 3*time.Second {
		t.Fatalf("age should track the clock, want 3s got %v", fr.Age)
	}

	// 2) Exporter becomes unreachable mid-run.
	srv.Close()

	// Several failing collections — daemon must not crash and must keep the window.
	for i := 0; i < 3; i++ {
		clk.advance(4 * time.Second)
		if err := d.Refresh(context.Background()); err == nil {
			t.Fatalf("Refresh against a closed exporter should surface an error")
		}
	}

	// Last-known value RETAINED: the published State is unchanged (same window,
	// same device count, refresh counter did not advance on failures).
	st2 := d.Snapshot()
	if st2 == nil || st2.Window == nil || st2.Window.Pack == nil {
		t.Fatalf("stale window must be RETAINED, not blanked")
	}
	if len(st2.Window.Pack.Mappings) != devCount {
		t.Fatalf("stale window must keep the last-known device values (had %d, now %d)", devCount, len(st2.Window.Pack.Mappings))
	}
	if st2.Refreshes != st.Refreshes {
		t.Fatalf("a failed collection must not advance the refresh counter (was %d, now %d)", st.Refreshes, st2.Refreshes)
	}

	// Now stale=true with a provenance reason, and age kept growing.
	fr = d.Freshness()
	if !fr.Stale {
		t.Fatalf("after exporter unreachable + age past threshold, data must be stale: %+v", fr)
	}
	if fr.Reason == "" {
		t.Fatalf("stale verdict must carry a provenance reason")
	}
	if fr.ConsecFails == 0 {
		t.Fatalf("stale reason should reflect consecutive collection failures")
	}
	if fr.Age <= fr.StalenessAfter {
		t.Fatalf("stale age (%v) must exceed the threshold (%v)", fr.Age, fr.StalenessAfter)
	}

	// 3) RECOVERY: a fresh successful collection clears staleness and serves live
	// again (the reason + streak reset on a successful publish).
	srv2 := dcgmFixtureServer(t)
	defer srv2.Close()
	dcgm.ScrapeURL = srv2.URL
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("recovery Refresh failed: %v", err)
	}
	if fr := d.Freshness(); fr.Stale {
		t.Fatalf("a successful collection must clear staleness: %+v", fr)
	} else if fr.ConsecFails != 0 || fr.Reason != "" {
		t.Fatalf("recovery must reset the failure streak/reason, got %+v", fr)
	}
}

// TestFreshnessStaleWhenMetricsDieButLogSourceAlive is the multi-collector
// regression the live smoke surfaced: the real agent runs a METRICS collector
// (DCGM/Prom) ALONGSIDE a log/event collector. When the exporter dies, the log
// source still answers, so the cycle is NOT a total failure — it publishes a
// window. Naively that would (a) reset freshness to "fresh" and (b) serve an
// EMPTY /cost (devices=null), silently dropping the last-known cost data. The
// daemon must instead measure freshness against the last METRICS-bearing cycle
// and RETAIN the last cost-bearing window, flagged stale (TASK-0040 / RULES §B).
func TestFreshnessStaleWhenMetricsDieButLogSourceAlive(t *testing.T) {
	srv := dcgmFixtureServer(t)
	defer srv.Close()

	clk := &manualClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}

	dcgm := &DCGMExporterCollector{ScrapeURL: srv.URL, Node: "lab-node"}
	// The log collector keeps answering even after the exporter dies (its fixture
	// source never errors), exactly as in the real agent's collector list.
	logCol := LogEventCollector{Src: FixtureLogSource{}, Node: "lab-node"}
	d := NewDaemon(DaemonConfig{
		AgentID:        "freshness-multi",
		Window:         time.Minute,
		StalenessAfter: 10 * time.Second,
		Collectors:     []Collector{dcgm, logCol},
		Policy:         DefaultCostPolicy(),
		Now:            clk.now,
	})

	// FRESH: both sources answer; a cost-bearing window is published.
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}
	cs := d.CostSnapshot()
	if cs == nil || cs.Window == nil || len(cs.Window.Pack.Mappings) == 0 {
		t.Fatalf("expected a cost-bearing window with device mappings")
	}
	devCount := len(cs.Window.Pack.Mappings)
	if fr := d.Freshness(); fr.Stale {
		t.Fatalf("fresh multi-collector cycle must not be stale: %+v", fr)
	}

	// Exporter dies; the log source stays alive so each cycle STILL publishes.
	srv.Close()
	for i := 0; i < 4; i++ {
		clk.advance(4 * time.Second)
		// Refresh returns nil here (the log source answered ⇒ not a total failure),
		// but it must NOT crash and must NOT reset cost freshness.
		_ = d.Refresh(context.Background())
	}

	// The freshest window advanced (log/XID cycle) — /signals stays live.
	if d.Snapshot().Refreshes < 2 {
		t.Fatalf("log-only cycles should still advance the freshest window")
	}
	// But the COST-bearing snapshot is RETAINED with its device values — not the
	// empty log-only window.
	cs2 := d.CostSnapshot()
	if cs2 == nil || cs2.Window == nil || len(cs2.Window.Pack.Mappings) != devCount {
		t.Fatalf("cost-bearing window must be retained (had %d mappings)", devCount)
	}
	// And it is now flagged STALE with a metrics-collection reason, age past
	// threshold — never served as live, never blanked.
	fr := d.Freshness()
	if !fr.Stale {
		t.Fatalf("metrics dead past threshold must be stale even with log source alive: %+v", fr)
	}
	if fr.Reason == "" || fr.ConsecFails == 0 {
		t.Fatalf("stale verdict must carry a metrics-failure reason + streak: %+v", fr)
	}
	if fr.Age <= fr.StalenessAfter {
		t.Fatalf("cost age (%v) must exceed threshold (%v) — measured against the metrics cycle", fr.Age, fr.StalenessAfter)
	}
}

// TestFreshnessNoDataBeforeFirstCollection proves freshness is well-defined
// before any collection: HasData=false, Stale=false (there is no held window to
// flag — distinct from holding stale data).
func TestFreshnessNoDataBeforeFirstCollection(t *testing.T) {
	d := NewDaemon(DaemonConfig{
		Window:         time.Minute,
		StalenessAfter: time.Second,
		Collectors:     DefaultCollectors("n"),
		Now:            fixedNow,
	})
	fr := d.Freshness()
	if fr.HasData {
		t.Fatalf("no collection yet ⇒ HasData must be false")
	}
	if fr.Stale {
		t.Fatalf("no data is not 'stale data' — Stale must be false, got %+v", fr)
	}
}

// TestDefaultStalenessAfter pins the derived default: max(3×cadence, 5s floor).
func TestDefaultStalenessAfter(t *testing.T) {
	if got := defaultStalenessAfter(time.Second); got != 5*time.Second {
		t.Errorf("small cadence must floor to 5s, got %v", got)
	}
	if got := defaultStalenessAfter(15 * time.Second); got != 45*time.Second {
		t.Errorf("3×15s = 45s, got %v", got)
	}
	// NewDaemon with no StalenessAfter applies the default from Window.
	d := NewDaemon(DaemonConfig{Window: 20 * time.Second, Collectors: DefaultCollectors("n"), Now: fixedNow})
	if d.stalenessAfter != 60*time.Second {
		t.Errorf("daemon default staleness should be 3×Window=60s, got %v", d.stalenessAfter)
	}
}

// TestHealthAndCostReportFreshness proves the /healthz and /cost JSON carry the
// freshness fields, and that the mock-default (no real endpoint, always-fresh)
// path reports stale=false with a small age — the backward-compatible green path.
func TestHealthAndCostReportFreshness(t *testing.T) {
	d := newTestDaemon(t) // mock default, fixed clock, one successful refresh
	srv := httptest.NewServer(NewAPI(d).Handler())
	defer srv.Close()

	// /healthz carries last_success_at + stale=false on the fresh mock default.
	hb := httpGet(t, srv.URL+"/healthz", http.StatusOK)
	var health HealthResponse
	if err := json.Unmarshal(hb, &health); err != nil {
		t.Fatalf("decode /healthz: %v", err)
	}
	if !health.OK {
		t.Fatalf("/healthz ok must be true")
	}
	if health.Stale {
		t.Fatalf("fresh mock default must not be stale on /healthz: %+v", health)
	}
	if health.LastSuccessAt.IsZero() {
		t.Fatalf("/healthz must report last_success_at after a successful collection")
	}

	// /cost carries collected_at + stale=false + a small age.
	cb := httpGet(t, srv.URL+"/cost", http.StatusOK)
	var cost CostResponse
	if err := json.Unmarshal(cb, &cost); err != nil {
		t.Fatalf("decode /cost: %v", err)
	}
	if cost.Stale {
		t.Fatalf("fresh mock default /cost must not be stale: %+v", cost)
	}
	if cost.CollectedAt.IsZero() {
		t.Fatalf("/cost must report collected_at")
	}
	if cost.StaleReason != "" {
		t.Fatalf("fresh /cost must carry no stale_reason, got %q", cost.StaleReason)
	}
}
