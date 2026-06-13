//go:build !gpu

package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// These tests are the TASK-0037 acceptance: with httptest Prometheus + DCGM
// endpoints the daemon uses the REAL MetricsChain (not the mock collectors); an
// unreachable endpoint degrades without crashing; and no endpoint keeps the
// mock default (demo1 unchanged). They run on the DEFAULT !gpu build with NO
// NVML — proving real HTTP collection needs no -tags gpu.

// realPromServer answers the DefaultPromQueries with one fixture device so the
// real PrometheusCollector returns data (and the chain selects Prometheus).
func realPromServer(t *testing.T) *httptest.Server {
	t.Helper()
	q := DefaultPromQueries()
	byQuery := map[string]string{
		q.PeakFLOPS:     vec(sample("GPU-real-0001", "lab-node-1", "", 1.25e14)),
		q.AchievedFLOPs: vec(sample("GPU-real-0001", "lab-node-1", "", 8.75e13)),
		q.TensorActive:  vec(sample("GPU-real-0001", "lab-node-1", "", 0.70)),
		q.CostPerHour:   vec(sample("GPU-real-0001", "lab-node-1", "", 1.10)),
		q.JobOwner:      vec(sample("GPU-real-0001", "lab-node-1", "job-real-a", 1)),
	}
	return promVectorServer(t, byQuery, nil)
}

// realDCGMServer serves a minimal DCGM-exporter text exposition.
func realDCGMServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("dcgm mock got non-GET %s (collector must be read-only)", r.Method)
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(
			`DCGM_FI_PROF_PIPE_TENSOR_ACTIVE{UUID="GPU-real-0002",Hostname="lab-node-1",modelName="A10"} 0.42` + "\n"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// refreshOnce drives one collection cycle and returns the published pack
// provenance so a test can assert which collector path actually ran.
func refreshOnce(t *testing.T, cols []Collector) map[string]string {
	t.Helper()
	d := NewDaemon(DaemonConfig{
		AgentID:    "gpufleet-agent",
		Window:     time.Minute,
		Collectors: cols,
		Policy:     DefaultCostPolicy(),
		Now:        fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()
	if st == nil || st.Window == nil || st.Window.Pack == nil {
		t.Fatalf("no published state")
	}
	return st.Window.Pack.Provenance
}

// TestRuntimeRealEndpointsUseRealChain asserts that with both endpoints
// configured the daemon runs the REAL MetricsChain — provenance carries the
// chain markers + real endpoint, and NOT the mock markers.
func TestRuntimeRealEndpointsUseRealChain(t *testing.T) {
	prom := realPromServer(t)
	dcgm := realDCGMServer(t)

	rc := RuntimeConfig{
		Node:            "lab-node-1",
		PrometheusURL:   prom.URL,
		DCGMExporterURL: dcgm.URL + "/metrics",
	}
	cols, mode := rc.Collectors()
	if mode != CollectorModeReal {
		t.Fatalf("auto mode with endpoints must pick real, got %q", mode)
	}
	prov := refreshOnce(t, cols)

	// Real chain selected Prometheus (primary answered) and tagged the real URL.
	if got := prov["prometheus.chain.selected"]; got != "primary" {
		t.Fatalf("expected real chain primary selection, got %q (prov=%v)", got, prov)
	}
	if got := prov["prometheus.endpoint"]; got != prom.URL {
		t.Fatalf("expected real prometheus endpoint %q, got %q", prom.URL, got)
	}
	if got := prov["prometheus.scrape_reused"]; got != "true" {
		t.Fatalf("expected real prometheus scrape_reused=true, got %q", got)
	}
	// And NOT the mock collector's provenance markers.
	if v, ok := prov["prometheus.promql"]; ok && strings.Contains(v, "job:gpu_owner") {
		t.Fatalf("mock prometheus provenance leaked: %q", v)
	}
	if v := prov["dcgm.exporter"]; strings.Contains(v, "mock") {
		t.Fatalf("mock dcgm provenance leaked: %q", v)
	}
}

// TestRuntimeDCGMFallbackWhenNoPrometheus asserts the chain falls back to the
// real DCGM scrape when only the DCGM endpoint is given (no Prometheus primary).
func TestRuntimeDCGMFallbackWhenNoPrometheus(t *testing.T) {
	dcgm := realDCGMServer(t)
	rc := RuntimeConfig{Node: "lab-node-1", DCGMExporterURL: dcgm.URL + "/metrics"}
	cols, mode := rc.Collectors()
	if mode != CollectorModeReal {
		t.Fatalf("DCGM-only must pick real, got %q", mode)
	}
	prov := refreshOnce(t, cols)
	// DCGM is the only source; the chain emits a DCGM-sourced observation.
	if got := prov["dcgm.source"]; got != "dcgm-exporter-scrape" {
		t.Fatalf("expected real dcgm scrape, got %q (prov=%v)", got, prov)
	}
}

// TestRuntimeUnreachableEndpointDegrades asserts that a garbage/unreachable
// endpoint does NOT crash the daemon: the chain has no data to answer with and
// the cycle degrades (no panic, no error escalation to a job).
func TestRuntimeUnreachableEndpointDegrades(t *testing.T) {
	// A closed server URL: connections refuse immediately.
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := closed.URL
	closed.Close() // now unreachable

	rc := RuntimeConfig{
		Node:            "lab-node-1",
		PrometheusURL:   url,
		DCGMExporterURL: url + "/metrics",
	}
	cols, mode := rc.Collectors()
	if mode != CollectorModeReal {
		t.Fatalf("endpoints given must pick real, got %q", mode)
	}

	d := NewDaemon(DaemonConfig{
		AgentID:    "gpufleet-agent",
		Window:     time.Minute,
		Collectors: cols,
		Policy:     DefaultCostPolicy(),
		Now:        fixedNow,
	})
	// Must not panic. The metrics chain (both sources down) fails and is counted
	// as a degrade; the build-agnostic log collector still answers (empty), so the
	// cycle publishes a degraded-but-valid window rather than crashing — RULES §A
	// off-path: a collector failure never affects a job.
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("unreachable endpoint must degrade, not error the whole cycle: %v", err)
	}
	if d.Errs() == 0 {
		t.Fatalf("expected the unreachable metrics endpoint to be counted as a degrade")
	}
	st := d.Snapshot()
	if st == nil || st.Window == nil || st.Window.Pack == nil {
		t.Fatalf("expected a degraded-but-valid window published despite unreachable metrics")
	}
	// Partial-collection provenance records the failed metrics source.
	if got := st.Window.Pack.Provenance["agent.partial_collection"]; got != "true" {
		t.Fatalf("expected partial_collection=true when metrics endpoint unreachable, prov=%v", st.Window.Pack.Provenance)
	}
	// Daemon is still usable: a subsequent Refresh also degrades cleanly.
	_ = d.Refresh(context.Background())
}

// TestRuntimeGarbageEndpointDegradesWithDataSource asserts that when one source
// is garbage but another answers, the daemon degrades the bad one and still
// publishes a window from the good one (never crashes, never blanks).
func TestRuntimeGarbageEndpointDegradesWithDataSource(t *testing.T) {
	// Prometheus serves non-JSON garbage (parse error → primary fails);
	// DCGM answers, so the chain falls back and still produces a window.
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not json at all <<<"))
	}))
	t.Cleanup(garbage.Close)
	dcgm := realDCGMServer(t)

	rc := RuntimeConfig{
		Node:            "lab-node-1",
		PrometheusURL:   garbage.URL,
		DCGMExporterURL: dcgm.URL + "/metrics",
	}
	cols, _ := rc.Collectors()
	prov := refreshOnce(t, cols) // must not panic; publishes a window
	if got := prov["dcgm.source"]; got != "dcgm-exporter-scrape" {
		t.Fatalf("expected fallback to real dcgm when prometheus is garbage, got %q (prov=%v)", got, prov)
	}
	if got := prov["dcgm.chain.selected"]; got != "fallback" {
		t.Fatalf("expected chain to record a fallback, got %q", got)
	}
}

// TestRuntimeNoEndpointKeepsMock asserts the backward-compatible default: with
// NO endpoint configured the daemon runs the MOCK DefaultCollectors (demo1
// unchanged). The mock prometheus provenance markers must be present.
func TestRuntimeNoEndpointKeepsMock(t *testing.T) {
	rc := RuntimeConfig{Node: "mock-node"}
	cols, mode := rc.Collectors()
	if mode != CollectorModeMock {
		t.Fatalf("no endpoint must keep mock, got %q", mode)
	}
	prov := refreshOnce(t, cols)
	if got := prov["prometheus.endpoint"]; !strings.Contains(got, "mock") {
		t.Fatalf("expected mock prometheus endpoint, got %q (prov=%v)", got, prov)
	}
	if _, ok := prov["dcgm.exporter"]; !ok {
		t.Fatalf("expected mock dcgm provenance, prov=%v", prov)
	}
	// The mock chain marker must be ABSENT (mock collectors are not the real chain).
	if _, ok := prov["prometheus.chain.selected"]; ok {
		t.Fatalf("mock path must not carry the real chain marker")
	}
}

// TestRuntimeModeForcing asserts --collectors=mock forces mock even with an
// endpoint, and --collectors=real forces real even with none (degrading).
func TestRuntimeModeForcing(t *testing.T) {
	prom := realPromServer(t)

	// mock forced despite an endpoint.
	mockForced := RuntimeConfig{Node: "n", PrometheusURL: prom.URL, Mode: CollectorModeMock}
	if _, mode := mockForced.Collectors(); mode != CollectorModeMock {
		t.Fatalf("collectors=mock must force mock, got %q", mode)
	}

	// real forced with no endpoint: builds the chain (which then degrades, never
	// crashes) — assert it does not panic on Refresh.
	realForced := RuntimeConfig{Node: "n", Mode: CollectorModeReal}
	cols, mode := realForced.Collectors()
	if mode != CollectorModeReal {
		t.Fatalf("collectors=real must force real, got %q", mode)
	}
	d := NewDaemon(DaemonConfig{Window: time.Minute, Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow})
	_ = d.Refresh(context.Background()) // empty chain degrades; must not panic
}
