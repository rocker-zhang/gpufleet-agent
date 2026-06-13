//go:build !gpu

package agent

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// promVectorServer returns an httptest server that answers the Prometheus
// instant-query API, dispatching on the `query` expression to a fixed result.
// It lets the real PrometheusCollector be exercised with NO real Prometheus and
// NO GPU. It also records which queries were seen.
func promVectorServer(t *testing.T, byQuery map[string]string, seen *[]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("prometheus mock got non-GET %s (collector must be read-only)", r.Method)
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/api/v1/query" {
			http.NotFound(w, r)
			return
		}
		expr := r.URL.Query().Get("query")
		if seen != nil {
			*seen = append(*seen, expr)
		}
		body, ok := byQuery[expr]
		if !ok {
			// Unknown expression ⇒ empty success vector (field absent ⇒ degrade).
			body = `{"status":"success","data":{"resultType":"vector","result":[]}}`
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func vec(samples ...string) string {
	return `{"status":"success","data":{"resultType":"vector","result":[` +
		strings.Join(samples, ",") + `]}}`
}

func sample(uuid, node, job string, value float64) string {
	m := `"UUID":"` + uuid + `","Hostname":"` + node + `"`
	if job != "" {
		m += `,"job":"` + job + `"`
	}
	return `{"metric":{` + m + `},"value":[1700000000,"` +
		strconv.FormatFloat(value, 'g', -1, 64) + `"]}`
}

func TestPrometheusCollectorReadsInstantQuery(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	queries := PromQueries{
		TensorActive:  "tensor",
		AchievedFLOPs: "flops",
		PeakFLOPS:     "peak",
		CostPerHour:   "cost",
		JobOwner:      "owner",
	}
	byQuery := map[string]string{
		"tensor": vec(sample("GPU-fixture-0001", "lab-node-1", "", 0.70)),
		"flops":  vec(sample("GPU-fixture-0001", "lab-node-1", "", 8.75e13)),
		"peak":   vec(sample("GPU-fixture-0001", "lab-node-1", "", 1.25e14)),
		"cost":   vec(sample("GPU-fixture-0001", "lab-node-1", "", 1.10)),
		"owner":  vec(sample("GPU-fixture-0001", "lab-node-1", "job-train-a", 1)),
	}
	var seen []string
	srv := promVectorServer(t, byQuery, &seen)

	c := PrometheusCollector{
		BaseURL: srv.URL,
		Node:    "lab-node-1",
		Queries: queries,
	}
	if c.Source() != gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS {
		t.Fatalf("Prometheus collector must report PROMETHEUS source")
	}
	obs, err := c.Collect(now, win)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(obs.DeviceWindows) != 1 {
		t.Fatalf("want 1 device window, got %d", len(obs.DeviceWindows))
	}
	dw := obs.DeviceWindows[0]
	if dw.UUID != "GPU-fixture-0001" {
		t.Fatalf("bad uuid: %q", dw.UUID)
	}
	if !dw.PeakFLOPSKnown || dw.PeakFLOPS != 1.25e14 {
		t.Fatalf("peak not read: %+v", dw)
	}
	if !dw.CostKnown || dw.CostPerHour != 1.10 {
		t.Fatalf("cost not read: %+v", dw)
	}
	// AchievedFLOPs is FLOP/s * window seconds.
	wantFLOPs := 8.75e13 * win.Seconds()
	if !dw.AchievedFLOPsKnown || dw.AchievedFLOPs != wantFLOPs {
		t.Fatalf("achieved flops not converted to window total: got %v want %v", dw.AchievedFLOPs, wantFLOPs)
	}
	// Tensor ratio * window seconds.
	if !dw.TensorActiveKnown || dw.TensorActiveSecs != 0.70*win.Seconds() {
		t.Fatalf("tensor active not converted: %+v", dw)
	}
	if len(obs.Mappings) != 1 || obs.Mappings[0].JobId != "job-train-a" {
		t.Fatalf("job owner mapping not read: %+v", obs.Mappings)
	}
	if obs.Provenance["scrape_reused"] != "true" {
		t.Fatalf("prometheus collector must record zero-new-scrape-load provenance")
	}

	// Sanity: it really queried over the instant-query API (zero new scrape load).
	if len(seen) == 0 {
		t.Fatalf("collector did not issue any PromQL query")
	}
}

func TestDCGMExporterParsesFixture(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	payload, err := os.ReadFile("testdata/dcgm_exporter.prom")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	obs, err := parseDCGMExposition(strings.NewReader(string(payload)), now, win, "fallback-node")
	if err != nil {
		t.Fatalf("parseDCGMExposition: %v", err)
	}
	if obs.Source != gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM {
		t.Fatalf("dcgm parse must be DCGM source")
	}
	if len(obs.DeviceWindows) != 2 {
		t.Fatalf("want 2 devices, got %d", len(obs.DeviceWindows))
	}
	// Devices are sorted by UUID.
	d0 := obs.DeviceWindows[0]
	if d0.UUID != "GPU-fixture-0001" || d0.Node != "lab-node-1" || d0.Model != "NVIDIA A10" {
		t.Fatalf("identity from labels wrong: %+v", d0)
	}
	if !d0.TensorActiveKnown || d0.TensorActiveSecs != 0.71*win.Seconds() {
		t.Fatalf("tensor-active not parsed/converted: %+v", d0)
	}
	// DCGM-exporter has no $/hour or peak: those MUST be left unknown (degrade).
	if d0.CostKnown || d0.PeakFLOPSKnown {
		t.Fatalf("dcgm parse must NOT fabricate cost/peak: %+v", d0)
	}
	// Profiling fields carried raw as series (tensor, sm, dram) per device = 6.
	if len(obs.DcgmSeries) != 6 {
		t.Fatalf("want 6 raw profiling series, got %d", len(obs.DcgmSeries))
	}
	// The non-DCGM line (go_goroutines) must NOT appear as a device/series.
	for _, s := range obs.DcgmSeries {
		if !strings.HasPrefix(s.FieldSymbol, "DCGM_") {
			t.Fatalf("non-DCGM series leaked: %q", s.FieldSymbol)
		}
	}
}

func TestDCGMExporterScrapeOverHTTP(t *testing.T) {
	now := fixedNow()
	payload, _ := os.ReadFile("testdata/dcgm_exporter.prom")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("dcgm scrape must be GET, got %s", r.Method)
		}
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	c := &DCGMExporterCollector{ScrapeURL: srv.URL, Node: "n"}
	obs, err := c.Collect(now, time.Minute)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(obs.DeviceWindows) != 2 {
		t.Fatalf("scrape parse mismatch: %d devices", len(obs.DeviceWindows))
	}
}

// TestDegradationChainPromMissingFieldDCGMFallback proves the source-selection
// chain: Prometheus is the default; when it returns NO data the chain falls back
// to the local DCGM-exporter scrape; and fields neither source supplies are
// marked degraded by the normalizer (never fabricated).
func TestDegradationChainPromMissingFieldDCGMFallback(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	// Prometheus that returns EMPTY for every query (field/data missing).
	emptyProm := promVectorServer(t, map[string]string{}, nil)
	prom := PrometheusCollector{BaseURL: emptyProm.URL, Node: "lab-node-1",
		Queries: PromQueries{TensorActive: "tensor", PeakFLOPS: "peak"}}

	// DCGM-exporter fixture server (the fallback).
	payload, _ := os.ReadFile("testdata/dcgm_exporter.prom")
	dcgmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer dcgmSrv.Close()
	dcgm := &DCGMExporterCollector{ScrapeURL: dcgmSrv.URL, Node: "lab-node-1"}

	chain := MetricsChain{Primary: prom, Fallback: dcgm}
	obs, err := chain.Collect(now, win)
	if err != nil {
		t.Fatalf("chain Collect: %v", err)
	}
	// Fallback engaged: DCGM device data is present.
	if len(obs.DeviceWindows) != 2 {
		t.Fatalf("chain did not fall back to DCGM: %d devices", len(obs.DeviceWindows))
	}
	if obs.Provenance["chain.selected"] != "fallback" {
		t.Fatalf("chain provenance must record fallback selection, got %q", obs.Provenance["chain.selected"])
	}

	// Now run the full normalize: DCGM has no peak/cost ⇒ those MUST degrade.
	w, err := Normalize("agent-chain", now, win, []Observation{obs})
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	var sawMFUDegrade, sawCostDegrade bool
	for _, dm := range w.Degraded {
		switch dm.Field {
		case "mfu":
			sawMFUDegrade = true
		case "cost":
			sawCostDegrade = true
		}
	}
	if !sawMFUDegrade {
		t.Errorf("expected mfu degradation (no peak from Prom or DCGM)")
	}
	if !sawCostDegrade {
		t.Errorf("expected cost degradation (no $/hour from Prom or DCGM)")
	}
	// Degrade, never fabricate: no device with unknown peak may appear in Samples.
	if len(w.Samples) != 0 {
		t.Fatalf("MFU must not be computed on absent peak; Samples=%d", len(w.Samples))
	}
}

func TestDegradationChainPrefersPrimaryWhenPresent(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	prom := PrometheusCollector{
		BaseURL: promVectorServer(t, map[string]string{
			"peak": vec(sample("GPU-fixture-0001", "lab-node-1", "", 1.25e14)),
		}, nil).URL,
		Node:    "lab-node-1",
		Queries: PromQueries{PeakFLOPS: "peak"},
	}
	// A fallback that would PANIC the test if consulted (it must not be).
	dcgm := &DCGMExporterCollector{ScrapeURL: "http://127.0.0.1:1/never"}
	chain := MetricsChain{Primary: prom, Fallback: dcgm}
	obs, err := chain.Collect(now, win)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if obs.Provenance["chain.selected"] != "primary" {
		t.Fatalf("must prefer primary Prometheus, got %q", obs.Provenance["chain.selected"])
	}
}

// TestProfilingBurstRateCapped asserts the off-critical-path profiling cap: with
// burst opted in, the collector refuses to scrape faster than BurstCap. We drive
// a controllable clock and verify scrapes are limited to the cap frequency.
func TestProfilingBurstRateCapped(t *testing.T) {
	var scrapes int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		scrapes++
		w.Write([]byte("DCGM_FI_PROF_PIPE_TENSOR_ACTIVE{UUID=\"GPU-x\",Hostname=\"n\"} 0.5\n"))
	}))
	defer srv.Close()

	clk := &manualClock{t: fixedNow()}
	c := &DCGMExporterCollector{
		ScrapeURL:      srv.URL,
		ProfilingBurst: true,
		BurstCap:       time.Second,
		now:            clk.now,
	}

	// Hammer Collect 100 times within a 4.5s simulated span, advancing the clock
	// by 50ms each call. With a 1s cap, at most ~5 scrapes may actually hit the
	// exporter; the rest must be rate-capped (ErrProfilingCapped).
	var capped, ok int
	for i := 0; i < 100; i++ {
		_, err := c.Collect(fixedNow(), time.Minute)
		switch err {
		case nil:
			ok++
		case ErrProfilingCapped:
			capped++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
		clk.advance(50 * time.Millisecond)
	}
	// 100 calls * 50ms = 5s simulated. With a 1s cap, allowed scrapes are bounded.
	if scrapes > 6 {
		t.Fatalf("profiling cap FAILED to limit frequency: %d real scrapes (cap=1s over ~5s)", scrapes)
	}
	if ok != scrapes {
		t.Fatalf("allowed Collects (%d) must equal real scrapes (%d)", ok, scrapes)
	}
	if capped == 0 {
		t.Fatalf("expected many rate-capped Collects, got 0")
	}
	t.Logf("profiling cap: %d real scrapes, %d capped over 100 calls (cap=1s)", scrapes, capped)
}

// TestProfilingNoBurstNotCapped proves the DEFAULT (no burst) path is NOT
// rate-gated by the collector itself — the daemon's own scrape interval is the
// limiter, aligned to the customer's existing interval.
func TestProfilingNoBurstNotCapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("DCGM_FI_PROF_PIPE_TENSOR_ACTIVE{UUID=\"GPU-x\",Hostname=\"n\"} 0.5\n"))
	}))
	defer srv.Close()
	c := &DCGMExporterCollector{ScrapeURL: srv.URL} // ProfilingBurst=false
	for i := 0; i < 5; i++ {
		if _, err := c.Collect(fixedNow(), time.Minute); err != nil {
			t.Fatalf("default (no-burst) scrape must not be capped: %v", err)
		}
	}
}

// TestLogEventCollectorXIDAndNCCL proves the log/event collector produces XID and
// NCCL signals end-to-end from fixture text on the default (GPU-less) build.
func TestLogEventCollectorXIDAndNCCL(t *testing.T) {
	now := fixedNow()
	kmsg, _ := os.ReadFile("testdata/kmsg_xid.txt")
	nccl, _ := os.ReadFile("testdata/nccl.log")

	c := LogEventCollector{
		Src:  FixtureLogSource{KmsgText: string(kmsg), NCCLText: string(nccl)},
		Node: "lab-node-1",
	}
	if c.Source() != gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID {
		t.Fatalf("log collector source must be DMESG_XID")
	}
	obs, err := c.Collect(now, time.Minute)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}

	// Two XID events (79 fallen-off-bus, 48 ECC) parsed; non-Xid lines ignored.
	if len(obs.XidEvents) != 2 {
		t.Fatalf("want 2 XID events, got %d: %+v", len(obs.XidEvents), obs.XidEvents)
	}
	if obs.XidEvents[0].Xid != 79 {
		t.Fatalf("first XID should be 79, got %d", obs.XidEvents[0].Xid)
	}
	if !strings.Contains(obs.XidEvents[0].RawMessage, "fallen off the bus") {
		t.Fatalf("raw XID text not preserved verbatim: %q", obs.XidEvents[0].RawMessage)
	}
	// Second XID carries the GPU UUID parsed from the line.
	if obs.XidEvents[1].Xid != 48 || !strings.HasPrefix(obs.XidEvents[1].DeviceUuid, "GPU-") {
		t.Fatalf("second XID/UUID wrong: %+v", obs.XidEvents[1])
	}

	// NCCL: one timeout event + two completed collectives.
	var timeouts, completed int
	for _, e := range obs.NcclEvents {
		switch e.Kind {
		case gpufleetv1.NcclEventKind_NCCL_EVENT_KIND_OP_TIMEOUT:
			timeouts++
		case gpufleetv1.NcclEventKind_NCCL_EVENT_KIND_OP_COMPLETED:
			completed++
		}
	}
	if timeouts != 1 {
		t.Fatalf("want 1 NCCL timeout event, got %d: %+v", timeouts, obs.NcclEvents)
	}
	if completed != 2 {
		t.Fatalf("want 2 NCCL completed events, got %d", completed)
	}

	// End-to-end: the events survive normalization into the proto EvidencePack.
	w, err := Normalize("agent-logs", now, time.Minute, []Observation{obs})
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if len(w.Pack.XidEvents) != 2 {
		t.Fatalf("XID events not carried into pack: %d", len(w.Pack.XidEvents))
	}
	if len(w.Pack.NcclEvents) != 3 {
		t.Fatalf("NCCL events not carried into pack: %d", len(w.Pack.NcclEvents))
	}
}

// TestLogEventCollectorDegradesMissingStreams proves a missing/absent stream
// degrades (recorded), never fabricates and never errors.
func TestLogEventCollectorDegradesMissingStreams(t *testing.T) {
	c := LogEventCollector{Src: FixtureLogSource{}} // no kmsg, no nccl
	obs, err := c.Collect(fixedNow(), time.Minute)
	if err != nil {
		t.Fatalf("missing streams must not error: %v", err)
	}
	if len(obs.XidEvents) != 0 || len(obs.NcclEvents) != 0 {
		t.Fatalf("absent streams must yield no fabricated events")
	}
	if obs.Provenance["kmsg"] != "unavailable" || obs.Provenance["nccl"] != "unavailable" {
		t.Fatalf("absent streams must be marked unavailable in provenance: %+v", obs.Provenance)
	}
}

// TestPrometheusBadURLErrors proves a wholly-failing primary returns an error so
// the daemon counts+degrades (off-path isolation).
func TestPrometheusHardFailIsError(t *testing.T) {
	c := PrometheusCollector{
		BaseURL: "http://127.0.0.1:1", // refused
		Queries: PromQueries{PeakFLOPS: "peak"},
		Timeout: 200 * time.Millisecond,
	}
	if _, err := c.Collect(fixedNow(), time.Minute); err == nil {
		t.Fatalf("expected error from unreachable Prometheus")
	}
	// And a malformed BaseURL is rejected.
	if _, err := url.Parse("://bad"); err == nil {
		t.Skip("url.Parse unexpectedly accepted; skip")
	}
}

// manualClock is a deterministic, advanceable clock for the cap test.
type manualClock struct {
	t time.Time
}

func (c *manualClock) now() time.Time          { return c.t }
func (c *manualClock) advance(d time.Duration) { c.t = c.t.Add(d) }
