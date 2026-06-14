//go:build !gpu

package agent

import (
	"context"
	"testing"
	"time"

	semantics "github.com/rocker-zhang/gpufleet-semantics"
)

// These are the TASK-0044 acceptance tests for the BUILT-IN GPU peak-FLOPS table.
// The strict precedence is exercised end-to-end on the DEFAULT !gpu build (NO
// NVML): (1) modelName "NVIDIA A10" with NO --peak-tflops auto-fills 125 from the
// table, MFU computes, provenance=builtin-table; (2) an operator --peak-tflops /
// spec overrides the table; (3) a real telemetry peak overrides BOTH; (4) an
// unknown model degrades (no fabrication); (5) the mock default is unchanged.

// costDevice finds a device's cost wedge by UUID in a CostReport, or reports
// absent (a device whose MFU degraded is omitted from the wedge, not fabricated).
func costDevice(rep CostReport, uuid string) (semantics.CostWedge, bool) {
	for _, d := range rep.Devices {
		if d.Device.Device.UUID == uuid {
			return d, true
		}
	}
	return semantics.CostWedge{}, false
}

// (1) HEADLINE DoD: modelName "NVIDIA A10", NO --peak-tflops, built-in table only.
// Peak auto-fills to 125 TFLOP/s, the device becomes MFU-computable, $/hr stays
// degraded (table is FLOPs-only), and provenance is stamped builtin-table.
func TestBuiltinPeakTableAutoFillsA10(t *testing.T) {
	dcgm := dcgmTensorOnlyServer(t, "GPU-a10-0001", "NVIDIA A10", 0.42)

	rc := RuntimeConfig{
		Node:            "lab-node-1",
		DCGMExporterURL: dcgm.URL + "/metrics",
		PeakTable:       true, // NO Spec, NO --peak-tflops
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

	sp, ok := st.Window.Specs["GPU-a10-0001"]
	if !ok {
		t.Fatalf("A10 should be MFU-computable after built-in-table peak fill; specs=%v", st.Window.Specs)
	}
	if sp.PeakFLOPS != 125e12 {
		t.Fatalf("table peak = %g, want 1.25e14 (125 TFLOP/s)", sp.PeakFLOPS)
	}
	// FLOPs-only: cost stays operator-supplied ⇒ unpriced (0), never fabricated.
	if sp.CostPerHour != 0 {
		t.Fatalf("built-in table must NOT supply cost; got %g", sp.CostPerHour)
	}

	// Real MFU computes: achieved = tensor_active * peak * window / window basis.
	// The device runs at 0.42 tensor-active so MFU is in (0,1).
	dc, ok := costDevice(st.Cost, "GPU-a10-0001")
	if !ok {
		t.Fatalf("A10 should appear in /cost after table peak fill")
	}
	if !(dc.Device.MFU > 0 && dc.Device.MFU < 1) {
		t.Fatalf("MFU should compute real in (0,1); got %g", dc.Device.MFU)
	}
	if dc.Impact.Computed {
		t.Fatalf("device must be UNPRICED (table is FLOPs-only, no $/hr)")
	}

	prov := st.Window.Pack.Provenance
	if got := prov["dcgm.builtin.peak.source"]; got != "builtin-table:A10@fp16-dense" {
		t.Fatalf("peak provenance = %q, want builtin-table:A10@fp16-dense", got)
	}
	if got := prov["dcgm.builtin.filled_devices"]; got != "GPU-a10-0001" {
		t.Fatalf("filled_devices = %q, want GPU-a10-0001", got)
	}
}

// (2) Operator --peak-tflops (spec) OVERRIDES the built-in table. The operator
// gives a deliberately DIFFERENT peak (300) and it wins; provenance is the spec
// source, NOT the table.
func TestOperatorPeakOverridesBuiltinTable(t *testing.T) {
	dcgm := dcgmTensorOnlyServer(t, "GPU-a10-0001", "NVIDIA A10", 0.42)

	// Homogeneous-box wildcard spec (the --peak-tflops shortcut), peak 300 != 125.
	spec, err := ParseDeviceSpec([]byte(`{"*": {"peak_tflops": 300}}`))
	if err != nil {
		t.Fatalf("ParseDeviceSpec: %v", err)
	}
	rc := RuntimeConfig{
		Node:            "lab-node-1",
		DCGMExporterURL: dcgm.URL + "/metrics",
		Spec:            spec,
		PeakTable:       true,
	}
	cols, _ := rc.Collectors()
	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()

	if got := st.Window.Specs["GPU-a10-0001"].PeakFLOPS; got != 300e12 {
		t.Fatalf("operator spec peak must win (300), got %g", got)
	}
	prov := st.Window.Pack.Provenance
	if got := prov["dcgm.spec.peak.source"]; got != "static-spec" {
		t.Fatalf("operator-explicit peak must stamp static-spec, got %q", got)
	}
	if _, ok := prov["dcgm.builtin.peak.source"]; ok {
		t.Fatalf("built-in table must NOT stamp when operator spec already filled the peak")
	}
}

// (3) A REAL telemetry peak overrides BOTH the operator spec and the built-in
// table. Prometheus supplies a real peak series (200); even with a spec (300)
// AND the table (125) enabled, the real series wins and nothing else stamps.
func TestRealPeakOverridesSpecAndBuiltinTable(t *testing.T) {
	// The default PromQL peak query is DCGM_FI_DEV_TENSOR_PEAK_FLOPS; supply it as a
	// REAL series (200 TFLOP/s) plus tensor-active. (sample's 3rd arg is the job
	// label; the model is irrelevant here since the real peak fills it directly.)
	q := DefaultPromQueries()
	prom := promVectorServer(t, map[string]string{
		q.TensorActive:  vec(sample("GPU-a10-0001", "lab-node-1", "", 0.5)),
		q.PeakFLOPS:     vec(sample("GPU-a10-0001", "lab-node-1", "", 200e12)),
		q.AchievedFLOPs: vec(sample("GPU-a10-0001", "lab-node-1", "", 0.5*200e12)),
	}, nil)

	spec, err := ParseDeviceSpec([]byte(`{"*": {"peak_tflops": 300}}`))
	if err != nil {
		t.Fatalf("ParseDeviceSpec: %v", err)
	}
	rc := RuntimeConfig{
		Node:          "lab-node-1",
		PrometheusURL: prom.URL,
		Spec:          spec,
		PeakTable:     true,
	}
	cols, _ := rc.Collectors()
	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()

	if got := st.Window.Specs["GPU-a10-0001"].PeakFLOPS; got != 200e12 {
		t.Fatalf("real telemetry peak must win (200), got %g", got)
	}
	prov := st.Window.Pack.Provenance
	if _, ok := prov["prometheus.spec.peak.source"]; ok {
		t.Fatalf("spec must NOT fill when a real peak series is present")
	}
	if _, ok := prov["prometheus.builtin.peak.source"]; ok {
		t.Fatalf("built-in table must NOT fill when a real peak series is present")
	}
}

// (4) UNKNOWN model DEGRADES — no fabrication. A model the table does not know
// leaves peak unknown, MFU degrades, the device is omitted from /cost, and no
// builtin-table provenance is stamped.
func TestUnknownModelDegradesNotFabricated(t *testing.T) {
	dcgm := dcgmTensorOnlyServer(t, "GPU-x-0001", "ACME SuperChip 9000", 0.42)

	rc := RuntimeConfig{
		Node:            "lab-node-1",
		DCGMExporterURL: dcgm.URL + "/metrics",
		PeakTable:       true,
	}
	cols, _ := rc.Collectors()
	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()

	if _, ok := st.Window.Specs["GPU-x-0001"]; ok {
		t.Fatalf("unknown model must NOT become MFU-computable (no fabricated peak)")
	}
	if _, ok := costDevice(st.Cost, "GPU-x-0001"); ok {
		t.Fatalf("unknown-model device must be omitted from /cost (peak degraded)")
	}
	if _, ok := st.Window.Pack.Provenance["dcgm.builtin.peak.source"]; ok {
		t.Fatalf("unknown model must NOT stamp builtin-table provenance")
	}
	// And the mfu degrade-mark must be present.
	var sawMFUDegrade bool
	for _, m := range st.Window.Degraded {
		if m.DeviceUUID == "GPU-x-0001" && m.Field == "mfu" {
			sawMFUDegrade = true
		}
	}
	if !sawMFUDegrade {
		t.Fatalf("unknown model must carry an mfu degrade-mark, got %+v", st.Window.Degraded)
	}
}

// (5) MOCK DEFAULT UNCHANGED: enabling the table on the no-endpoint mock default
// is a transparent passthrough (mock devices have no real datacenter modelName),
// so the demo1 mock path is byte-for-byte unchanged and no builtin stamp appears.
func TestBuiltinTableMockDefaultUnchanged(t *testing.T) {
	rcBase := RuntimeConfig{Node: "lab-node-1"} // no endpoints ⇒ mock
	rcTable := RuntimeConfig{Node: "lab-node-1", PeakTable: true}

	run := func(rc RuntimeConfig) *State {
		cols, mode := rc.Collectors()
		if mode != CollectorModeMock {
			t.Fatalf("no endpoint must pick mock, got %q", mode)
		}
		d := NewDaemon(DaemonConfig{
			AgentID: "gpufleet-agent", Window: time.Minute,
			Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
		})
		if err := d.Refresh(context.Background()); err != nil {
			t.Fatalf("Refresh: %v", err)
		}
		return d.Snapshot()
	}

	base := run(rcBase)
	withTable := run(rcTable)

	if len(base.Cost.Devices) != len(withTable.Cost.Devices) {
		t.Fatalf("mock device count changed: base=%d table=%d",
			len(base.Cost.Devices), len(withTable.Cost.Devices))
	}
	for _, k := range []string{"dcgm.builtin.peak.source", "prometheus.builtin.peak.source"} {
		if _, ok := withTable.Window.Pack.Provenance[k]; ok {
			t.Fatalf("mock default must not trigger a builtin-table fill (%s present)", k)
		}
	}
}

// TestBuiltinTableDisabledDegrades asserts the --builtin-peak-table OFF path:
// with PeakTable=false the table never fills, so even a known modelName ("NVIDIA
// A10") DEGRADES exactly like an unknown model — peak stays unknown, the device is
// omitted from /cost, and no builtin-table provenance is stamped. This is the
// "operator opts out for pure degrade-not-fabricate" contract and the inverse of
// TestBuiltinPeakTableAutoFillsA10 (which proves the default-ON fill).
func TestBuiltinTableDisabledDegrades(t *testing.T) {
	dcgm := dcgmTensorOnlyServer(t, "GPU-a10-0001", "NVIDIA A10", 0.42)

	rc := RuntimeConfig{
		Node:            "lab-node-1",
		DCGMExporterURL: dcgm.URL + "/metrics",
		PeakTable:       false, // operator disabled the table (env GPUFLEET_BUILTIN_PEAK_TABLE=false)
	}
	cols, _ := rc.Collectors()
	d := NewDaemon(DaemonConfig{
		AgentID: "gpufleet-agent", Window: time.Minute,
		Collectors: cols, Policy: DefaultCostPolicy(), Now: fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()

	if _, ok := st.Window.Specs["GPU-a10-0001"]; ok {
		t.Fatalf("table OFF: known A10 must NOT become MFU-computable (no auto-fill)")
	}
	if _, ok := costDevice(st.Cost, "GPU-a10-0001"); ok {
		t.Fatalf("table OFF: A10 must be omitted from /cost (peak degraded)")
	}
	if _, ok := st.Window.Pack.Provenance["dcgm.builtin.peak.source"]; ok {
		t.Fatalf("table OFF: no builtin-table provenance may be stamped")
	}
}

// TestMatchModelPeak unit-tests the tolerant model matcher: case-insensitivity,
// the NVIDIA/Tesla prefix, SKU suffixes, and degrade-on-unknown.
func TestMatchModelPeak(t *testing.T) {
	cases := []struct {
		in        string
		wantCanon string
		wantTFL   float64
		wantOK    bool
	}{
		{"NVIDIA A10", "A10", 125, true},
		{"nvidia a10", "A10", 125, true},
		{"A10", "A10", 125, true},
		{"NVIDIA A100-SXM4-80GB", "A100-80GB", 312, true},
		{"NVIDIA A100-PCIE-40GB", "A100-40GB", 312, true},
		{"A100 80GB", "A100-80GB", 312, true},
		{"NVIDIA H100 80GB HBM3", "H100-SXM", 989.4, true},
		{"NVIDIA H100 PCIe", "H100-PCIe", 756, true},
		{"NVIDIA H100 NVL", "H100-NVL", 835, true}, // distinct per-GPU dense figure
		{"NVIDIA GH200", "GH200", 989.4, true},     // Grace-Hopper, shares H100 GH100 die
		{"NVIDIA GH200 480GB", "GH200", 989.4, true},
		{"NVIDIA L4", "L4", 121, true},
		{"NVIDIA L40", "L40", 181, true},
		{"NVIDIA L40S", "L40S", 362, true}, // must prefer L40S over L40
		{"Tesla V100-SXM2-16GB", "V100", 125, true},
		{"Tesla T4", "T4", 65, true},
		{"NVIDIA RTX A6000", "A6000", 155, true},
		// GB10 is DELIBERATELY ABSENT (no FP16/BF16-dense datasheet figure; the
		// "1 PFLOP" headline is FP4+sparsity marketing). It must DEGRADE, not match.
		{"NVIDIA GB10", "", 0, false},
		{"GB10", "", 0, false},
		{"NVIDIA B200", "B200", 2250, true},
		{"NVIDIA H200", "H200", 989.4, true},
		// Unknowns DEGRADE (never guessed):
		{"ACME SuperChip 9000", "", 0, false},
		{"", "", 0, false},
		{"NVIDIA", "", 0, false},
		{"L5", "", 0, false},    // not a real key; must not fuzzy-hit L4
		{"A1000", "", 0, false}, // must not hit A10 / A100
	}
	for _, c := range cases {
		canon, tfl, ok := matchModelPeak(c.in)
		if ok != c.wantOK || canon != c.wantCanon || tfl != c.wantTFL {
			t.Errorf("matchModelPeak(%q) = (%q, %g, %v), want (%q, %g, %v)",
				c.in, canon, tfl, ok, c.wantCanon, c.wantTFL, c.wantOK)
		}
	}
}
