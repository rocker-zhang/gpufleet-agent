//go:build !gpu

package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These are the TASK-0038 acceptance tests: a static device-spec fills missing
// peak/cost (so MFU + $/hr render real) stamped static-spec; degrade-not-
// fabricate holds when neither a real series nor a spec is present; a query/label
// override actually takes effect; a real series wins over the spec; and the
// no-endpoint mock default is unchanged. They run on the DEFAULT !gpu build with
// NO NVML.

// dcgmTensorOnlyServer serves a vanilla A10 dcgm-exporter exposition: ONLY
// tensor-active (no peak, no cost) — exactly the D-0017 NO-GO box where $/hr and
// MFU silently degrade to 0 without a spec.
func dcgmTensorOnlyServer(t *testing.T, uuid, model string, tensorActive float64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("dcgm mock got non-GET %s (collector must be read-only)", r.Method)
			http.Error(w, "read-only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		line := `DCGM_FI_PROF_PIPE_TENSOR_ACTIVE{UUID="` + uuid + `",Hostname="lab-node-1",modelName="` + model + `"} ` +
			strconv.FormatFloat(tensorActive, 'g', -1, 64) + "\n"
		_, _ = w.Write([]byte(line))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestDeviceSpecFillsMFUAndCostFromStaticSpec is the headline DoD test: with a
// static A10 spec (peak+rate) and a real DCGM tensor-active series (httptest) but
// NO real peak/cost, the device becomes MFU-computable, $/hr renders > 0, and the
// origin is stamped static-spec for auditability.
func TestDeviceSpecFillsMFUAndCostFromStaticSpec(t *testing.T) {
	dcgm := dcgmTensorOnlyServer(t, "GPU-a10-0001", "NVIDIA A10", 0.42)

	spec, err := ParseDeviceSpec([]byte(`{"NVIDIA A10": {"peak_tflops": 125, "cost_usd_per_hour": 1.20}}`))
	if err != nil {
		t.Fatalf("ParseDeviceSpec: %v", err)
	}

	rc := RuntimeConfig{
		Node:            "lab-node-1",
		DCGMExporterURL: dcgm.URL + "/metrics",
		Spec:            spec,
	}
	cols, mode := rc.Collectors()
	if mode != CollectorModeReal {
		t.Fatalf("endpoint given must pick real, got %q", mode)
	}

	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()
	if st == nil || st.Window == nil {
		t.Fatalf("no published state")
	}

	// Peak now known ⇒ MFU computable ⇒ device is in Samples with a real spec.
	sp, ok := st.Window.Specs["GPU-a10-0001"]
	if !ok {
		t.Fatalf("device should be MFU-computable after static-spec peak fill; specs=%v", st.Window.Specs)
	}
	if sp.PeakFLOPS != 125e12 {
		t.Fatalf("spec peak = %g, want 1.25e14 (125 TFLOP/s)", sp.PeakFLOPS)
	}
	if sp.CostPerHour != 1.20 {
		t.Fatalf("spec cost = %g, want 1.20", sp.CostPerHour)
	}

	// $/hr renders real (priced, > 0): the device runs below 100% MFU so idle
	// fraction > 0 and the wedge attributes priced wasted spend from the spec rate.
	if len(st.Cost.Devices) != 1 {
		t.Fatalf("expected one priced device wedge, got %d", len(st.Cost.Devices))
	}
	dev := st.Cost.Devices[0]
	if !dev.Impact.Computed {
		t.Fatalf("expected wedge priced (Computed) from static-spec cost, got %+v", dev.Impact)
	}
	if dev.Impact.UsdPerHour <= 0 {
		t.Fatalf("expected $/hr > 0 from static-spec cost, got %g (impact=%+v)", dev.Impact.UsdPerHour, dev.Impact)
	}

	// Provenance stamps the static-spec origin (namespaced by the dcgm source).
	prov := st.Window.Pack.Provenance
	if got := prov["dcgm.spec.peak.source"]; got != "static-spec" {
		t.Fatalf("expected peak.source=static-spec, prov=%v", prov)
	}
	if got := prov["dcgm.spec.cost.source"]; got != "static-spec" {
		t.Fatalf("expected cost.source=static-spec, prov=%v", prov)
	}
	if got := prov["dcgm.spec.filled_devices"]; got != "GPU-a10-0001" {
		t.Fatalf("expected filled_devices=GPU-a10-0001, got %q", got)
	}
}

// TestDeviceSpecAbsentStillDegrades is the degrade-not-fabricate regression:
// the same vanilla tensor-only box with NO spec must leave peak/cost unknown —
// the device is NOT MFU-computable and $/hr stays 0 (never invented).
func TestDeviceSpecAbsentStillDegrades(t *testing.T) {
	dcgm := dcgmTensorOnlyServer(t, "GPU-a10-0002", "NVIDIA A10", 0.42)

	rc := RuntimeConfig{Node: "lab-node-1", DCGMExporterURL: dcgm.URL + "/metrics"} // NO Spec
	cols, _ := rc.Collectors()
	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()

	// No peak ⇒ MFU NOT computable ⇒ device absent from Samples (never faked).
	if _, ok := st.Window.Specs["GPU-a10-0002"]; ok {
		t.Fatalf("without a spec the peak-less device must degrade, not be MFU-computable")
	}
	// $/hr stays unpriced (no device wedge at all, since MFU is not computable
	// without a peak): nothing fabricated.
	if len(st.Cost.Devices) != 0 {
		t.Fatalf("expected no priced device wedge when neither real series nor spec present, got %d", len(st.Cost.Devices))
	}
	// And the degrade marks record the missing inputs (auditable, not invented).
	var sawMFU, sawCost bool
	for _, dm := range st.Window.Degraded {
		if dm.DeviceUUID == "GPU-a10-0002" && dm.Field == "mfu" {
			sawMFU = true
		}
		if dm.DeviceUUID == "GPU-a10-0002" && dm.Field == "cost" {
			sawCost = true
		}
	}
	if !sawMFU || !sawCost {
		t.Fatalf("expected mfu+cost degrade marks with no spec, got %+v", st.Window.Degraded)
	}
	// No static-spec provenance must be stamped when there is no spec.
	if _, ok := st.Window.Pack.Provenance["dcgm.spec.peak.source"]; ok {
		t.Fatalf("no spec must not stamp a static-spec provenance")
	}
}

// TestRealSeriesWinsOverSpec asserts the precedence rule: when a real Prometheus
// series DOES supply peak/cost, the spec is NOT used (the real metered values win)
// and no static-spec provenance is stamped for those fields.
func TestRealSeriesWinsOverSpec(t *testing.T) {
	// Prometheus supplies a REAL peak + cost + tensor + achieved for the device.
	q := DefaultPromQueries()
	byQuery := map[string]string{
		q.PeakFLOPS:     vec(sample("GPU-real-0003", "lab-node-1", "", 2.0e14)), // real 200 TFLOP/s
		q.AchievedFLOPs: vec(sample("GPU-real-0003", "lab-node-1", "", 1.0e14)),
		q.TensorActive:  vec(sample("GPU-real-0003", "lab-node-1", "", 0.50)),
		q.CostPerHour:   vec(sample("GPU-real-0003", "lab-node-1", "", 3.00)), // real $3.00/hr
		q.JobOwner:      vec(sample("GPU-real-0003", "lab-node-1", "job-x", 1)),
	}
	prom := promVectorServer(t, byQuery, nil)

	// Spec would supply DIFFERENT (wrong) numbers — must be ignored.
	spec, err := ParseDeviceSpec([]byte(`{"by_uuid": {"GPU-real-0003": {"peak_tflops": 125, "cost_usd_per_hour": 1.20}}}`))
	if err != nil {
		t.Fatalf("ParseDeviceSpec: %v", err)
	}

	rc := RuntimeConfig{Node: "lab-node-1", PrometheusURL: prom.URL, Spec: spec}
	cols, _ := rc.Collectors()
	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()
	sp := st.Window.Specs["GPU-real-0003"]
	if sp.PeakFLOPS != 2.0e14 {
		t.Fatalf("real peak must win over spec: got %g, want 2e14", sp.PeakFLOPS)
	}
	if sp.CostPerHour != 3.00 {
		t.Fatalf("real cost must win over spec: got %g, want 3.00", sp.CostPerHour)
	}
	// No static-spec stamp because the spec filled nothing (real won both fields).
	if _, ok := st.Window.Pack.Provenance["prometheus.spec.peak.source"]; ok {
		t.Fatalf("real series present ⇒ spec must not stamp peak.source")
	}
	if _, ok := st.Window.Pack.Provenance["prometheus.spec.cost.source"]; ok {
		t.Fatalf("real series present ⇒ spec must not stamp cost.source")
	}
}

// TestSpecFillsOnlyMissingField asserts the partial-fill case: when a real series
// supplies peak but NOT cost, the spec fills ONLY cost (real peak preserved) and
// stamps cost.source=static-spec but not peak.source.
func TestSpecFillsOnlyMissingField(t *testing.T) {
	q := DefaultPromQueries()
	byQuery := map[string]string{
		q.PeakFLOPS:     vec(sample("GPU-mix-0004", "lab-node-1", "", 2.0e14)), // real peak
		q.AchievedFLOPs: vec(sample("GPU-mix-0004", "lab-node-1", "", 1.0e14)),
		q.TensorActive:  vec(sample("GPU-mix-0004", "lab-node-1", "", 0.10)),
		// NO CostPerHour series (vanilla box) ⇒ cost degrades without spec.
		q.JobOwner: vec(sample("GPU-mix-0004", "lab-node-1", "job-y", 1)),
	}
	prom := promVectorServer(t, byQuery, nil)
	spec, _ := ParseDeviceSpec([]byte(`{"by_uuid": {"GPU-mix-0004": {"peak_tflops": 125, "cost_usd_per_hour": 2.50}}}`))

	rc := RuntimeConfig{Node: "lab-node-1", PrometheusURL: prom.URL, Spec: spec}
	cols, _ := rc.Collectors()
	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()
	sp := st.Window.Specs["GPU-mix-0004"]
	if sp.PeakFLOPS != 2.0e14 {
		t.Fatalf("real peak must be preserved: got %g", sp.PeakFLOPS)
	}
	if sp.CostPerHour != 2.50 {
		t.Fatalf("spec must fill the missing cost: got %g", sp.CostPerHour)
	}
	prov := st.Window.Pack.Provenance
	if _, ok := prov["prometheus.spec.peak.source"]; ok {
		t.Fatalf("peak came from a real series; peak.source must NOT be stamped static-spec")
	}
	if got := prov["prometheus.spec.cost.source"]; got != "static-spec" {
		t.Fatalf("cost was spec-filled; expected cost.source=static-spec, prov=%v", prov)
	}
}

// TestQueryAndLabelOverrideTakesEffect asserts the operator can realign the
// PromQL expression AND the identity label keys to a non-default dcgm-exporter
// schema without rebuilding — and that the override actually drives collection.
func TestQueryAndLabelOverrideTakesEffect(t *testing.T) {
	// A box whose schema differs from the defaults: custom metric names and a
	// custom UUID label key "gpu" instead of "UUID".
	const customTensor = "my_custom_tensor_active"
	const customPeak = "my_custom_peak_flops"
	var seen []string
	byQuery := map[string]string{
		customTensor: `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"gpu":"GPU-ovr-0005","host":"lab-node-1"},"value":[1700000000,"0.30"]}]}}`,
		customPeak: `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"gpu":"GPU-ovr-0005","host":"lab-node-1"},"value":[1700000000,"1e14"]}]}}`,
		"my_custom_achieved": `{"status":"success","data":{"resultType":"vector","result":[` +
			`{"metric":{"gpu":"GPU-ovr-0005","host":"lab-node-1"},"value":[1700000000,"5e13"]}]}}`,
	}
	prom := promVectorServer(t, byQuery, &seen)

	rc := RuntimeConfig{
		Node:          "lab-node-1",
		PrometheusURL: prom.URL,
		Queries: PromQueries{
			TensorActive:  customTensor,
			PeakFLOPS:     customPeak,
			AchievedFLOPs: "my_custom_achieved",
			// CostPerHour/JobOwner intentionally left to "" (skipped) for this box.
		},
		Labels: PromLabels{UUID: "gpu", Node: "host"},
	}
	cols, _ := rc.Collectors()
	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	// The custom expressions were actually queried (override drove collection).
	joined := strings.Join(seen, "|")
	if !strings.Contains(joined, customTensor) || !strings.Contains(joined, customPeak) {
		t.Fatalf("override queries were not used; seen=%v", seen)
	}
	// The default expressions must NOT have been queried.
	if strings.Contains(joined, "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE") {
		t.Fatalf("default tensor query leaked despite override; seen=%v", seen)
	}

	// And the custom UUID label key resolved the device (MFU computable from the
	// custom peak+achieved+tensor), proving the label override took effect.
	st := d.Snapshot()
	if _, ok := st.Window.Specs["GPU-ovr-0005"]; !ok {
		t.Fatalf("custom UUID label key did not resolve the device; specs=%v", st.Window.Specs)
	}
}

// TestNoSpecNoEndpointMockUnchanged is the backward-compat regression: with no
// spec and no endpoint the mock default path is byte-for-byte unchanged (the
// SpecFillCollector is a pure passthrough when the spec is empty).
func TestNoSpecNoEndpointMockUnchanged(t *testing.T) {
	withSpec := RuntimeConfig{Node: "mock-node"}        // empty spec
	baseCols, baseMode := withSpec.Collectors()
	if baseMode != CollectorModeMock {
		t.Fatalf("no endpoint must keep mock, got %q", baseMode)
	}
	// No collector in the mock default must be wrapped by SpecFillCollector.
	for _, c := range baseCols {
		if _, wrapped := c.(SpecFillCollector); wrapped {
			t.Fatalf("empty spec must NOT wrap the mock default (backward compat)")
		}
	}
	prov := refreshOnce(t, baseCols)
	if got := prov["prometheus.endpoint"]; !strings.Contains(got, "mock") {
		t.Fatalf("expected mock prometheus endpoint unchanged, got %q", got)
	}
	if _, ok := prov["prometheus.chain.selected"]; ok {
		t.Fatalf("mock path must not carry the real chain marker")
	}
}

// TestLoadDeviceSpecFlatAndExplicitShapes asserts the JSON loader accepts both
// the flat per-model shape and the explicit {by_uuid,by_model} shape, with UUID
// winning over model and tolerant model matching.
func TestLoadDeviceSpecFlatAndExplicitShapes(t *testing.T) {
	dir := t.TempDir()
	flat := filepath.Join(dir, "flat.json")
	if err := os.WriteFile(flat, []byte(`{"NVIDIA A10": {"peak_tflops": 125, "cost_usd_per_hour": 1.20}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := LoadDeviceSpec(flat)
	if err != nil {
		t.Fatalf("LoadDeviceSpec flat: %v", err)
	}
	// Tolerant model matching: "A10" and "nvidia a10" both hit the "NVIDIA A10" entry.
	if e, ok := s.lookup("", "A10"); !ok || e.PeakTFLOPS != 125 {
		t.Fatalf("tolerant model match failed for A10: %+v ok=%v", e, ok)
	}
	if e, ok := s.lookup("", "nvidia a10"); !ok || e.CostPerHour != 1.20 {
		t.Fatalf("tolerant model match failed for nvidia a10: %+v ok=%v", e, ok)
	}

	explicit := filepath.Join(dir, "explicit.json")
	body := `{"by_uuid": {"GPU-pin-0006": {"peak_tflops": 312, "cost_usd_per_hour": 4.10}},` +
		`"by_model": {"NVIDIA A10": {"peak_tflops": 125, "cost_usd_per_hour": 1.20}}}`
	if err := os.WriteFile(explicit, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	s2, err := LoadDeviceSpec(explicit)
	if err != nil {
		t.Fatalf("LoadDeviceSpec explicit: %v", err)
	}
	// UUID wins over model when both could match.
	if e, ok := s2.lookup("GPU-pin-0006", "NVIDIA A10"); !ok || e.PeakTFLOPS != 312 {
		t.Fatalf("UUID entry must win over model: %+v ok=%v", e, ok)
	}

	// Empty path ⇒ empty spec, no error.
	empty, err := LoadDeviceSpec("")
	if err != nil || !empty.Empty() {
		t.Fatalf("empty path must yield empty spec no error, got %+v err=%v", empty, err)
	}
}

// TestShippedA10SpecLoads keeps the documented example spec (testdata,
// referenced from CLAUDE.md §5c) valid and parseable.
func TestShippedA10SpecLoads(t *testing.T) {
	s, err := LoadDeviceSpec(filepath.Join("testdata", "device-spec-a10.json"))
	if err != nil {
		t.Fatalf("LoadDeviceSpec testdata A10: %v", err)
	}
	e, ok := s.lookup("", "NVIDIA A10")
	if !ok || e.PeakTFLOPS != 125 || e.CostPerHour != 1.20 {
		t.Fatalf("shipped A10 spec wrong: %+v ok=%v", e, ok)
	}
}
