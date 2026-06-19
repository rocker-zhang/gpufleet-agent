//go:build !gpu

package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the TASK-0053 SECOND-source surface for XID79, NCCL, and the
// Prometheus-primary ECC path. Each leg is emitted ONLY from a genuinely-collected
// fact attributed to the real observing Source (G1), and the end-to-end tests run
// through the REAL rca engine (registry.NewDefaultEngine via evalVerdict) to prove
// FIRE on two distinct sources and ABSTAIN on one / on a forged single collector.

// ---------------------------------------------------------------------------
// (1) XID79 device.lost.<addr>@PROC — the non-dmesg 2nd leg.
// ---------------------------------------------------------------------------

// writeNVIDIASysfsDevice lays out a fake NVIDIA sysfs PCIe device dir with the
// given vendor + link width attributes (empty value ⇒ file omitted).
func writeNVIDIASysfsDevice(t *testing.T, root, addr, vendor, curW, maxW string) {
	t.Helper()
	dir := filepath.Join(root, addr)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, v string) {
		if v == "" {
			return
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(v), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("vendor", vendor)
	write("current_link_width", curW)
	write("max_link_width", maxW)
}

// TestProcDeviceLostEmitsLeg: an NVIDIA GPU whose current link width is 0 (link
// fully down) with a known max>0 ⇒ device.lost.<addr>@PROC.
func TestProcDeviceLostEmitsLeg(t *testing.T) {
	now := fixedNow()
	root := t.TempDir()
	writeNVIDIASysfsDevice(t, root, "0000:0a:00.0", "0x10de\n", "0\n", "16\n")

	obs, err := ProcLinkCollector{SysfsRoot: root, Node: "n"}.Collect(now, time.Minute)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(obs.Timeline) != 1 {
		t.Fatalf("want 1 device.lost leg, got %d: %+v", len(obs.Timeline), obs.Timeline)
	}
	te := obs.Timeline[0]
	if !strings.HasPrefix(te.GetSignalId(), "device.lost.") {
		t.Fatalf("wrong signal id: %q", te.GetSignalId())
	}
	if te.GetSource() != gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC {
		t.Fatalf("device.lost leg must be @PROC, got %v", te.GetSource())
	}
}

// TestProcDeviceLostNonNVIDIANoLeg: a NON-NVIDIA device with width 0 must NOT mint
// a device.lost leg (we cannot attribute a lost GPU to a non-GPU slot).
func TestProcDeviceLostNonNVIDIANoLeg(t *testing.T) {
	now := fixedNow()
	root := t.TempDir()
	writeNVIDIASysfsDevice(t, root, "0000:0b:00.0", "0x8086\n", "0\n", "16\n") // Intel vendor
	obs, _ := ProcLinkCollector{SysfsRoot: root}.Collect(now, time.Minute)
	if len(obs.Timeline) != 0 {
		t.Fatalf("non-NVIDIA width-0 device must emit NO device.lost leg, got %+v", obs.Timeline)
	}
}

// TestProcDeviceLostPresentLinkNoLeg: an NVIDIA GPU with a healthy up link (curW>0)
// must NOT mint a device.lost leg (it is present, not lost). A degraded width is a
// LINK_DEGRADED concern, not device.lost.
func TestProcDeviceLostPresentLinkNoLeg(t *testing.T) {
	now := fixedNow()
	root := t.TempDir()
	writeNVIDIASysfsDevice(t, root, "0000:0c:00.0", "0x10de\n", "16\n", "16\n")
	obs, _ := ProcLinkCollector{SysfsRoot: root}.Collect(now, time.Minute)
	for _, te := range obs.Timeline {
		if strings.HasPrefix(te.GetSignalId(), "device.lost.") {
			t.Fatalf("present NVIDIA GPU must emit NO device.lost leg, got %q", te.GetSignalId())
		}
	}
}

// TestProcDeviceLostMissingVendorDegrades: width 0 but no vendor file ⇒ cannot
// confirm NVIDIA ⇒ no leg (degrade, never fabricate).
func TestProcDeviceLostMissingVendorDegrades(t *testing.T) {
	now := fixedNow()
	root := t.TempDir()
	writeNVIDIASysfsDevice(t, root, "0000:0d:00.0", "", "0\n", "16\n") // vendor omitted
	obs, _ := ProcLinkCollector{SysfsRoot: root}.Collect(now, time.Minute)
	if len(obs.Timeline) != 0 {
		t.Fatalf("missing vendor must emit NO device.lost leg, got %+v", obs.Timeline)
	}
}

// xid79DmesgCollector is a genuine DMESG_XID collector emitting an Xid 79 line (as
// a real /dev/kmsg tail would on a GPU that fell off the bus).
type xid79DmesgCollector struct{ uuid string }

func (xid79DmesgCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID
}
func (c xid79DmesgCollector) Collect(now time.Time, _ time.Duration) (Observation, error) {
	return Observation{
		Source: gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
		XidEvents: []*gpufleetv1.XidEvent{{
			Ts: timestamppb.New(now), DeviceUuid: c.uuid, Xid: 79,
			RawMessage: "NVRM: Xid (PCI:0000:0a:00): 79, GPU has fallen off the bus", Severity: "err",
		}},
	}, nil
}

// TestE2E_XID79FiresOnTwoSources: a dmesg.xid79@DMESG_XID leg + an INDEPENDENT
// device.lost@PROC leg (two distinct sources) → the REAL engine FIRES
// GPU_FALLEN_OFF_BUS.
func TestE2E_XID79FiresOnTwoSources(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	root := t.TempDir()
	writeNVIDIASysfsDevice(t, root, "0000:0a:00.0", "0x10de\n", "0\n", "16\n")

	cols := []Collector{
		xid79DmesgCollector{uuid: "GPU-felloff-0001"},
		ProcLinkCollector{SysfsRoot: root, Node: "n"},
	}
	obs := collectAll(t, cols, now, win)
	w := mustNormalize(t, now, win, obs)
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_GPU_FALLEN_OFF_BUS {
		t.Fatalf("two-source XID79 must FIRE GPU_FALLEN_OFF_BUS, got %v", v.GetFaultClass())
	}
}

// TestE2E_XID79OneLegAbstains: only the dmesg leg (no PROC device.lost), and only
// the PROC leg (no dmesg) → ABSTAIN in both directions.
func TestE2E_XID79OneLegAbstains(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	// dmesg leg only.
	obs := collectAll(t, []Collector{xid79DmesgCollector{uuid: "GPU-solo"}}, now, win)
	w := mustNormalize(t, now, win, obs)
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("dmesg-only XID79 must ABSTAIN, got %v", v.GetFaultClass())
	}

	// PROC device.lost leg only.
	root := t.TempDir()
	writeNVIDIASysfsDevice(t, root, "0000:0a:00.0", "0x10de\n", "0\n", "16\n")
	obs2 := collectAll(t, []Collector{ProcLinkCollector{SysfsRoot: root}}, now, win)
	w2 := mustNormalize(t, now, win, obs2)
	if v := evalVerdict(w2); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("PROC-only device.lost must ABSTAIN, got %v", v.GetFaultClass())
	}
}

// TestE2E_XID79ForgedOneSourceAbstains: a SINGLE DMESG_XID collector emits the
// real xid79 line AND tries to mint the device.lost leg (claiming a PROC source it
// does not own). The provenance guard drops the forged PROC leg → ABSTAIN.
func TestE2E_XID79ForgedOneSourceAbstains(t *testing.T) {
	now := fixedNow()
	win := time.Minute

	forge := mockTimelineCollector{
		src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
		entries: []*gpufleetv1.TimelineEntry{
			{Source: gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID, SignalId: "dmesg.xid79.GPU-forge"},
			// A single DMESG_XID collector claims the independent PROC leg.
			{Source: gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC, SignalId: "device.lost.0000-0a-00-0"},
		},
	}
	obs := collectAll(t, []Collector{forge}, now, win)
	w := mustNormalize(t, now, win, obs)
	if _, ok := timelineIDs(w)["device.lost.0000-0a-00-0"]; ok {
		t.Fatalf("forged PROC device.lost from a DMESG_XID collector must be dropped by the provenance guard")
	}
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("forged single-source XID79 must ABSTAIN, got %v", v.GetFaultClass())
	}
}

// ---------------------------------------------------------------------------
// (2) NCCL nccl.timeout.<scope>@NCCL — genuine NCCL-source collector.
// ---------------------------------------------------------------------------

// TestNCCLLogCollectorSourceAttribution: the dedicated NCCLLogCollector reads a
// real NCCL log file and emits the watchdog timeout event source-tagged @NCCL,
// which normalize mints as nccl.timeout.<scope>@NCCL (G1: a genuine NCCL source).
func TestNCCLLogCollectorSourceAttribution(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	dir := t.TempDir()
	path := filepath.Join(dir, "nccl.log")
	logTxt := "[Rank 3] NCCL WARN comm 0xabc AllReduce watchdog timeout after 1800000 ms\n"
	if err := os.WriteFile(path, []byte(logTxt), 0o644); err != nil {
		t.Fatal(err)
	}

	c := NCCLLogCollector{Path: path, Node: "n"}
	if c.Source() != gpufleetv1.SignalSource_SIGNAL_SOURCE_NCCL {
		t.Fatalf("NCCL collector must be @NCCL")
	}
	obs, err := c.Collect(now, win)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(obs.NcclEvents) != 1 || obs.NcclEvents[0].GetKind() != gpufleetv1.NcclEventKind_NCCL_EVENT_KIND_OP_TIMEOUT {
		t.Fatalf("want 1 NCCL OP_TIMEOUT event, got %+v", obs.NcclEvents)
	}

	w := mustNormalize(t, now, win, []Observation{obs})
	ids := timelineIDs(w)
	if src, ok := ids["nccl.timeout.0xabc"]; !ok {
		t.Fatalf("nccl.timeout leg not emitted from genuine NCCL collector; ids=%v", ids)
	} else if src != gpufleetv1.SignalSource_SIGNAL_SOURCE_NCCL {
		t.Fatalf("nccl.timeout leg must be @NCCL, got %v", src)
	}
}

// TestNCCLLogCollectorAbsentDegrades: an empty/missing NCCL path emits no events
// (degrade, never fabricate, never error).
func TestNCCLLogCollectorAbsentDegrades(t *testing.T) {
	now := fixedNow()
	obs, err := NCCLLogCollector{Path: ""}.Collect(now, time.Minute)
	if err != nil {
		t.Fatalf("absent NCCL path must not error: %v", err)
	}
	if len(obs.NcclEvents) != 0 {
		t.Fatalf("absent NCCL log must emit no events, got %+v", obs.NcclEvents)
	}
	if obs.Provenance["nccl"] != "unavailable" {
		t.Fatalf("absent NCCL log must mark provenance unavailable, got %q", obs.Provenance["nccl"])
	}
}

// TestNCCLEventsOnDmesgSourceGatedOut: NCCL events that ride a DMESG_XID
// observation (the historical path) are CORRECTLY gated out of the nccl.timeout
// leg by G1 — only a genuine NCCL source mints it.
func TestNCCLEventsOnDmesgSourceGatedOut(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	// A DMESG_XID-sourced observation carrying an NCCL OP_TIMEOUT (forged source).
	obs := Observation{
		Source: gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
		NcclEvents: []*gpufleetv1.NcclEvent{{
			Ts: timestamppb.New(now), CommunicatorId: "0xdead",
			Kind: gpufleetv1.NcclEventKind_NCCL_EVENT_KIND_OP_TIMEOUT,
		}},
	}
	w := mustNormalize(t, now, win, []Observation{obs})
	if _, ok := timelineIDs(w)["nccl.timeout.0xdead"]; ok {
		t.Fatalf("NCCL event on a DMESG_XID source must NOT mint an nccl.timeout@NCCL leg (G1)")
	}
}

// TestE2E_NCCLTimeoutAbstainsNoCorroborator is the HONEST gap test: a genuine
// nccl.timeout@NCCL leg with NO independent collective.stall corroborator (none is
// genuinely collectable today) → the engine ABSTAINs. We emit ONLY the one real
// leg and never fabricate the second source (RULES §B).
func TestE2E_NCCLTimeoutAbstainsNoCorroborator(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	dir := t.TempDir()
	path := filepath.Join(dir, "nccl.log")
	if err := os.WriteFile(path, []byte("NCCL WARN comm 0xabc watchdog timeout\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	obs := collectAll(t, []Collector{NCCLLogCollector{Path: path}}, now, win)
	w := mustNormalize(t, now, win, obs)
	// The real NCCL leg IS present (honest single source).
	if _, ok := timelineIDs(w)["nccl.timeout.0xabc"]; !ok {
		t.Fatalf("expected the real nccl.timeout leg to be emitted")
	}
	// No collective.stall corroborator was fabricated.
	for id := range timelineIDs(w) {
		if strings.HasPrefix(id, "collective.stall") {
			t.Fatalf("fabricated collective.stall corroborator %q with no real source", id)
		}
	}
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("single-source NCCL timeout must ABSTAIN (no real corroborator), got %v", v.GetFaultClass())
	}
}

// ---------------------------------------------------------------------------
// (3) Prometheus-primary ECC-DBE — ecc.dbe.<uuid>@PROMETHEUS.
// ---------------------------------------------------------------------------

// TestPrometheusECCDoubleBitEmitsLeg: a Prometheus increase() query returning a
// per-window ECC double-bit delta>0 mints ecc.dbe.<uuid>@PROMETHEUS; a zero/absent
// increase emits no leg (degrade, never fabricate).
func TestPrometheusECCDoubleBitEmitsLeg(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-prom-ecc-0001"

	byQuery := map[string]string{
		"ecc_increase": vec(sample(uuid, "lab-node", "", 3)), // delta of 3 this window
	}
	srv := promVectorServer(t, byQuery, nil)
	c := PrometheusCollector{
		BaseURL: srv.URL,
		Node:    "lab-node",
		Queries: PromQueries{ECCDoubleBit: "ecc_increase"},
	}
	obs, err := c.Collect(now, win)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(obs.DeviceWindows) != 1 || !obs.DeviceWindows[0].ECCDoubleBitKnown {
		t.Fatalf("ECC delta not parsed/known: %+v", obs.DeviceWindows)
	}
	if got := obs.DeviceWindows[0].ECCDoubleBitErrs; got != 3 {
		t.Fatalf("want ECC delta 3, got %d", got)
	}
	w := mustNormalize(t, now, win, []Observation{obs})
	ids := timelineIDs(w)
	if src, ok := ids["ecc.dbe."+uuid]; !ok {
		t.Fatalf("ecc.dbe leg not emitted on Prometheus delta>0; ids=%v", ids)
	} else if src != gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS {
		t.Fatalf("Prometheus ECC leg must be @PROMETHEUS, got %v", src)
	}
}

// TestPrometheusECCZeroIncreaseNoLeg: a zero increase (no new error this window)
// leaves ECCDoubleBitKnown false ⇒ no leg.
func TestPrometheusECCZeroIncreaseNoLeg(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	byQuery := map[string]string{"ecc_increase": vec(sample("GPU-x", "n", "", 0))}
	srv := promVectorServer(t, byQuery, nil)
	c := PrometheusCollector{BaseURL: srv.URL, Node: "n", Queries: PromQueries{ECCDoubleBit: "ecc_increase"}}
	obs, _ := c.Collect(now, win)
	if len(obs.DeviceWindows) != 1 {
		t.Fatalf("want 1 device window, got %+v", obs.DeviceWindows)
	}
	if obs.DeviceWindows[0].ECCDoubleBitKnown {
		t.Fatalf("zero ECC increase must leave ECCDoubleBitKnown false")
	}
}

// promECCCollector is a genuine PROMETHEUS collector emitting an ECC double-bit
// delta>0 for a device (as a real Prometheus increase() query would).
type promECCCollector struct {
	uuid  string
	delta uint64
}

func (promECCCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS
}
func (c promECCCollector) Collect(time.Time, time.Duration) (Observation, error) {
	return Observation{
		Source: gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS,
		DeviceWindows: []DeviceWindow{
			{UUID: c.uuid, ECCDoubleBitErrs: c.delta, ECCDoubleBitKnown: true},
		},
	}, nil
}

// TestE2E_ECCFiresOnPrometheusPrimary: a PROMETHEUS ecc.dbe counter leg + an
// INDEPENDENT dmesg ECC Xid leg (two distinct sources, NO DCGM scrape) → the REAL
// engine FIRES ECC_UNCORRECTABLE on a Prometheus-primary node.
func TestE2E_ECCFiresOnPrometheusPrimary(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-prom-fire"

	cols := []Collector{
		promECCCollector{uuid: uuid, delta: 2},
		MockDmesgCollector{Node: "n", Inject: []*gpufleetv1.XidEvent{
			{Ts: timestamppb.New(now), DeviceUuid: uuid, Xid: 94, RawMessage: "Xid 94 ecc", Severity: "err"},
		}},
	}
	obs := collectAll(t, cols, now, win)
	w := mustNormalize(t, now, win, obs)
	v := evalVerdict(w)
	if v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ECC_UNCORRECTABLE {
		t.Fatalf("Prom-primary ECC must FIRE ECC_UNCORRECTABLE, got %v", v.GetFaultClass())
	}
	// The cited counter leg is genuinely @PROMETHEUS + the corroborator @DMESG_XID.
	srcs := map[gpufleetv1.SignalSource]bool{}
	for _, cs := range v.GetCitedSignals() {
		srcs[cs.GetSource()] = true
	}
	if !srcs[gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS] || !srcs[gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID] {
		t.Fatalf("Prom-primary ECC must cite PROMETHEUS counter + DMESG_XID corroborator, got %v", srcs)
	}
}

// TestE2E_ECCPrometheusForgedOneSourceAbstains: a SINGLE PROMETHEUS collector
// emits both the ECC counter AND tries to mint the dmesg corroborator (claiming a
// DMESG_XID source it does not own). The provenance guard drops the forged leg →
// ABSTAIN (one real source).
func TestE2E_ECCPrometheusForgedOneSourceAbstains(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-prom-forge"

	forge := forgePromECCCollector{
		uuid:  uuid,
		delta: 2,
		forgedLeg: &gpufleetv1.TimelineEntry{
			Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID, // claims dmesg it does not own
			SignalId: "dmesg.xid.ecc.94." + uuid,
		},
	}
	obs := collectAll(t, []Collector{forge}, now, win)
	w := mustNormalize(t, now, win, obs)
	if _, ok := timelineIDs(w)["dmesg.xid.ecc.94."+uuid]; ok {
		t.Fatalf("forged DMESG_XID leg from a PROMETHEUS collector must be dropped by the provenance guard")
	}
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("forged single-source Prom ECC must ABSTAIN, got %v", v.GetFaultClass())
	}
}

// forgePromECCCollector is a single PROMETHEUS collector that emits a real ECC
// delta AND a pre-formed leg claiming a foreign (DMESG_XID) source.
type forgePromECCCollector struct {
	uuid      string
	delta     uint64
	forgedLeg *gpufleetv1.TimelineEntry
}

func (forgePromECCCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS
}
func (c forgePromECCCollector) Collect(time.Time, time.Duration) (Observation, error) {
	return Observation{
		Source:        gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS,
		DeviceWindows: []DeviceWindow{{UUID: c.uuid, ECCDoubleBitErrs: c.delta, ECCDoubleBitKnown: true}},
		Timeline:      []*gpufleetv1.TimelineEntry{c.forgedLeg},
	}, nil
}
