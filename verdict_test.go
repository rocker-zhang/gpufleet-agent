//go:build !gpu

package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// timelineIDs returns the set of non-empty signal_ids emitted on the window's
// timeline, the citable evidence the rca gate matches on.
func timelineIDs(w *SignalWindow) map[string]gpufleetv1.SignalSource {
	out := map[string]gpufleetv1.SignalSource{}
	for _, te := range w.Pack.GetTimeline() {
		if te.GetSignalId() != "" {
			out[te.GetSignalId()] = te.GetSource()
		}
	}
	return out
}

// TestEmitSignalIDsPerFaultKind asserts normalize emits the correct
// device-attributed signal_id + Source for each fault kind it can derive from
// genuinely collected data — XID79, ECC-XID, NCCL timeout, DCGM ECC counter —
// matching the public rca playbook prefixes EXACTLY.
func TestEmitSignalIDsPerFaultKind(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	dcgm := MockDCGMCollector{
		Node:         "n",
		InjectECCDBE: map[string]uint64{"GPU-mock-0001": 2},
	}
	dmesg := MockDmesgCollector{Node: "n", Inject: []*gpufleetv1.XidEvent{
		{Ts: timestamppb.New(now), DeviceUuid: "GPU-mock-0002", Xid: 79, RawMessage: "Xid 79 off bus", Severity: "err"},
		{Ts: timestamppb.New(now), DeviceUuid: "GPU-mock-0003", Xid: 94, RawMessage: "Xid 94 ecc", Severity: "err"},
	}}
	// An NCCL OP_TIMEOUT carried via the DMESG_XID log collector's NcclEvents path
	// is sourced NCCL on its own leg; emit one here by injecting through a custom
	// observation source.
	nccl := mockNcclCollector{events: []*gpufleetv1.NcclEvent{
		{Ts: timestamppb.New(now), CommunicatorId: "0xabc", Rank: 3, Op: "AllReduce", Kind: gpufleetv1.NcclEventKind_NCCL_EVENT_KIND_OP_TIMEOUT},
	}}

	obs := collectAll(t, []Collector{dcgm, dmesg, nccl}, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	ids := timelineIDs(w)

	want := map[string]gpufleetv1.SignalSource{
		"dmesg.xid79.GPU-mock-0002":      gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
		"dmesg.xid.ecc.94.GPU-mock-0003": gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
		"nccl.timeout.0xabc":             gpufleetv1.SignalSource_SIGNAL_SOURCE_NCCL,
		"ecc.dbe.GPU-mock-0001":          gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
	}
	for id, src := range want {
		got, ok := ids[id]
		if !ok {
			t.Errorf("missing emitted signal_id %q; got ids=%v", id, ids)
			continue
		}
		if got != src {
			t.Errorf("signal_id %q: source = %v, want %v", id, got, src)
		}
	}
}

// mockNcclCollector emits NCCL-sourced events for tests (the real NCCL leg rides
// the log collector, but its independence axis is the NCCL source).
type mockNcclCollector struct{ events []*gpufleetv1.NcclEvent }

func (mockNcclCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_NCCL
}
func (c mockNcclCollector) Collect(now time.Time, _ time.Duration) (Observation, error) {
	return Observation{Source: c.Source(), NcclEvents: c.events}, nil
}

// TestNoFabricationWhenLegAbsent is the load-bearing honesty test: when only ONE
// real leg of a fault is present (XID79 dmesg with NO independent DCGM device-lost
// leg), the agent emits exactly that one real signal and NEVER synthesizes the
// missing corroborator — so the gate correctly ABSTAINs.
func TestNoFabricationWhenLegAbsent(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	// XID79 dmesg leg only; default DCGM (no device.lost injection).
	cols := []Collector{
		MockDCGMCollector{Node: "n"},
		MockPrometheusCollector{Node: "n"},
		MockDmesgCollector{Node: "n", Inject: []*gpufleetv1.XidEvent{
			{Ts: timestamppb.New(now), DeviceUuid: "GPU-mock-0001", Xid: 79, RawMessage: "Xid 79", Severity: "err"},
		}},
	}
	obs := collectAll(t, cols, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	ids := timelineIDs(w)

	// The real XID79 leg IS present.
	if _, ok := ids["dmesg.xid79.GPU-mock-0001"]; !ok {
		t.Fatalf("expected the real xid79 leg to be emitted")
	}
	// NO device.lost / ecc.dbe / nccl corroborator was fabricated.
	for id := range ids {
		if strings.HasPrefix(id, "device.lost") || strings.HasPrefix(id, "ecc.dbe") || strings.HasPrefix(id, "nccl.timeout") {
			t.Errorf("fabricated corroborator leg %q with no real source", id)
		}
	}

	// The gate must ABSTAIN on the single real leg.
	v := evalVerdict(w)
	if v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("single-source XID79 must ABSTAIN, got %v", v.GetFaultClass())
	}
}

// TestGateFiresOnInjectedECC asserts the ECC-uncorrectable signature FIRES on the
// injected 2-independent-source pattern (DCGM counter + dmesg ECC Xid), citing
// exactly the two real legs at confidence >= 0.95.
func TestGateFiresOnInjectedECC(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	cols := []Collector{
		MockDCGMCollector{Node: "n", InjectECCDBE: map[string]uint64{"GPU-mock-0002": 5}},
		MockPrometheusCollector{Node: "n"},
		MockDmesgCollector{Node: "n", Inject: []*gpufleetv1.XidEvent{
			{Ts: timestamppb.New(now), DeviceUuid: "GPU-mock-0002", Xid: 94, RawMessage: "Xid 94 ECC", Severity: "err"},
		}},
	}
	obs := collectAll(t, cols, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	v := evalVerdict(w)

	if v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ECC_UNCORRECTABLE {
		t.Fatalf("want ECC_UNCORRECTABLE, got %v", v.GetFaultClass())
	}
	if v.GetConfidence() < 0.95 {
		t.Fatalf("confidence = %v, want >= 0.95", v.GetConfidence())
	}
	cited := citedIDs(v)
	wantLegs := map[string]bool{
		"ecc.dbe.GPU-mock-0002":          false,
		"dmesg.xid.ecc.94.GPU-mock-0002": false,
	}
	for _, id := range cited {
		if _, ok := wantLegs[id]; ok {
			wantLegs[id] = true
		}
	}
	for id, seen := range wantLegs {
		if !seen {
			t.Errorf("ECC verdict missing cited real leg %q (cited=%v)", id, cited)
		}
	}
	// Independence: the two cited legs are from DIFFERENT sources.
	srcs := map[gpufleetv1.SignalSource]bool{}
	for _, cs := range v.GetCitedSignals() {
		srcs[cs.GetSource()] = true
	}
	if len(srcs) < 2 {
		t.Fatalf("ECC verdict cited < 2 distinct sources: %v", srcs)
	}
}

// TestGateFiresOnInjectedXID79 asserts the XID79 signature FIRES on the injected
// 2-independent-source pattern (dmesg Xid 79 + an independent DCGM device-lost
// leg) and yields GPU_FALLEN_OFF_BUS.
func TestGateFiresOnInjectedXID79(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	cols := []Collector{
		MockDCGMCollector{Node: "n", InjectTimeline: []*gpufleetv1.TimelineEntry{{
			Ts:         timestamppb.New(now),
			Source:     gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
			DeviceUuid: "GPU-mock-0001",
			SignalId:   "device.lost.dcgm.GPU-mock-0001",
			Label:      "DCGM health: device unreachable",
		}}},
		MockPrometheusCollector{Node: "n"},
		MockDmesgCollector{Node: "n", Inject: []*gpufleetv1.XidEvent{
			{Ts: timestamppb.New(now), DeviceUuid: "GPU-mock-0001", Xid: 79, RawMessage: "Xid 79 off bus", Severity: "err"},
		}},
	}
	obs := collectAll(t, cols, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	v := evalVerdict(w)
	if v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_GPU_FALLEN_OFF_BUS {
		t.Fatalf("want GPU_FALLEN_OFF_BUS, got %v", v.GetFaultClass())
	}
	if v.GetConfidence() < 0.95 {
		t.Fatalf("confidence = %v, want >= 0.95", v.GetConfidence())
	}
}

// TestProvenanceIntegrityDropsForeignSourceLeg asserts a collector cannot stamp a
// timeline leg onto a source it does not own: a DMESG_XID collector injecting a
// DCGM-sourced leg has that leg dropped (it would forge the independence axis).
func TestProvenanceIntegrityDropsForeignSourceLeg(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	// A dmesg-sourced collector tries to inject a DCGM-sourced device.lost leg.
	rogue := mockTimelineCollector{
		src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
		entries: []*gpufleetv1.TimelineEntry{{
			Ts:       timestamppb.New(now),
			Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM, // foreign!
			SignalId: "device.lost.dcgm.GPU-mock-0001",
		}},
	}
	obs := collectAll(t, []Collector{rogue}, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if _, ok := timelineIDs(w)["device.lost.dcgm.GPU-mock-0001"]; ok {
		t.Fatalf("a collector stamped a leg on a source it does not own; must be dropped")
	}
}

type mockTimelineCollector struct {
	src     gpufleetv1.SignalSource
	entries []*gpufleetv1.TimelineEntry
}

func (c mockTimelineCollector) Source() gpufleetv1.SignalSource { return c.src }
func (c mockTimelineCollector) Collect(_ time.Time, _ time.Duration) (Observation, error) {
	return Observation{Source: c.src, Timeline: c.entries}, nil
}

func citedIDs(v *gpufleetv1.Verdict) []string {
	var out []string
	for _, cs := range v.GetCitedSignals() {
		out = append(out, cs.GetSignalId())
	}
	return out
}

// TestDefaultMockAbstains asserts the default (clean) mock window ABSTAINs — no
// fault legs, so no class fires.
func TestDefaultMockAbstains(t *testing.T) {
	d := newTestDaemon(t)
	st := d.Snapshot()
	if st.Verdict == nil {
		t.Fatalf("default window should carry a (ABSTAIN) verdict")
	}
	if st.Verdict.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("default mock must ABSTAIN, got %v", st.Verdict.GetFaultClass())
	}
}

// newFiringDaemon builds a refreshed daemon on the fault-injecting mock sources.
func newFiringDaemon(t *testing.T) *Daemon {
	t.Helper()
	d := NewDaemon(DaemonConfig{
		AgentID:    "agent-fire",
		Window:     time.Minute,
		Collectors: FaultInjectCollectors("fire-node"),
		Policy:     DefaultCostPolicy(),
		Now:        fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	return d
}

// TestVerdictEndpoint asserts /verdict serves canonical gpufleet.v1 protojson, is
// GET-only (405 on POST), and is 503 before the first window.
func TestVerdictEndpoint(t *testing.T) {
	// 503 before the first window.
	cold := NewDaemon(DaemonConfig{AgentID: "cold", Window: time.Minute, Collectors: DefaultCollectors("n"), Now: fixedNow})
	csrv := httptest.NewServer(NewAPI(cold).Handler())
	defer csrv.Close()
	httpGet(t, csrv.URL+"/verdict", http.StatusServiceUnavailable)

	// A firing window: /verdict parses back to a real Verdict with a fired class.
	d := newFiringDaemon(t)
	srv := httptest.NewServer(NewAPI(d).Handler())
	defer srv.Close()

	body := httpGet(t, srv.URL+"/verdict", http.StatusOK)
	v := &gpufleetv1.Verdict{}
	if err := protojson.Unmarshal(body, v); err != nil {
		t.Fatalf("/verdict is not a valid gpufleet.v1 Verdict: %v", err)
	}
	// FaultInjectCollectors injects both ECC + XID79; the engine returns the first
	// match in registry order (XID79 before ECC).
	if v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_GPU_FALLEN_OFF_BUS {
		t.Fatalf("/verdict fault_class = %v, want GPU_FALLEN_OFF_BUS", v.GetFaultClass())
	}
	if v.GetConfidence() < 0.95 {
		t.Fatalf("/verdict confidence = %v, want >= 0.95", v.GetConfidence())
	}
	if len(v.GetCitedSignals()) < 2 {
		t.Fatalf("/verdict must cite >= 2 signals, got %d", len(v.GetCitedSignals()))
	}

	// 405 on a mutating verb.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/verdict", strings.NewReader("{}"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /verdict: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /verdict: got %d, want 405", resp.StatusCode)
	}
}

// TestWindowEmbedsVerdict asserts /window embeds the verdict as protojson.
func TestWindowEmbedsVerdict(t *testing.T) {
	d := newFiringDaemon(t)
	srv := httptest.NewServer(NewAPI(d).Handler())
	defer srv.Close()

	wb := httpGet(t, srv.URL+"/window", http.StatusOK)
	// The /window body must carry a "verdict" object that parses as a Verdict.
	var raw struct {
		Verdict json.RawMessage `json:"verdict"`
	}
	if err := json.Unmarshal(wb, &raw); err != nil {
		t.Fatalf("decode /window: %v", err)
	}
	if len(raw.Verdict) == 0 {
		t.Fatalf("/window did not embed a verdict")
	}
	v := &gpufleetv1.Verdict{}
	if err := protojson.Unmarshal(raw.Verdict, v); err != nil {
		t.Fatalf("/window embedded verdict is not a valid Verdict: %v", err)
	}
	if v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_GPU_FALLEN_OFF_BUS {
		t.Fatalf("/window verdict class = %v, want GPU_FALLEN_OFF_BUS", v.GetFaultClass())
	}
}
