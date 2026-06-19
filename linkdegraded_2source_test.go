//go:build !gpu

package agent

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file is the TASK-0053 second-source (LINK_DEGRADED) test surface. It
// proves both real legs are emitted from genuinely-collected telemetry and that
// the REAL rca engine FIRES only when the two legs come from two DISTINCT sources
// (DCGM link.error + PROC link.degraded), ABSTAINing otherwise.

// --- DCGM link-error delta leg (collectors_dcgm.go + normalize.go) ---

// dcgmLinkExposition renders a minimal DCGM-exporter text payload carrying the
// public NVLink/PCIe link-error counters for one device at a given lifetime sum.
func dcgmLinkExposition(uuid string, crc, replay, pcie int) string {
	var b strings.Builder
	lbl := `{UUID="` + uuid + `",Hostname="lab-node",modelName="NVIDIA H100"}`
	b.WriteString(dcgmSymNVLinkCRCFlitErr + lbl + " " + strconv.Itoa(crc) + "\n")
	b.WriteString(dcgmSymNVLinkReplayErr + lbl + " " + strconv.Itoa(replay) + "\n")
	b.WriteString(dcgmSymPCIeReplay + lbl + " " + strconv.Itoa(pcie) + "\n")
	return b.String()
}

// TestDCGMLinkErrorDeltaEmitsLeg: a cumulative link-error counter that GROWS
// across two scrapes yields a delta>0 → `link.error.<uuid>`@DCGM; the first
// (baseline) scrape and a flat counter emit nothing (honesty: a single reading
// is not evidence of a new error).
func TestDCGMLinkErrorDeltaEmitsLeg(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-link-0001"
	c := &DCGMExporterCollector{} // parse-only; we drive applyLinkErrDelta directly via Collect-less path

	// First scrape: establishes the baseline ⇒ zero delta.
	obs1, err := parseDCGMExposition(strings.NewReader(dcgmLinkExposition(uuid, 5, 2, 1)), now, win, "lab-node")
	if err != nil {
		t.Fatalf("parse1: %v", err)
	}
	if len(obs1.DeviceWindows) != 1 || !obs1.DeviceWindows[0].LinkErrorsKnown {
		t.Fatalf("link-error sum not parsed/known: %+v", obs1.DeviceWindows)
	}
	if got := obs1.DeviceWindows[0].LinkErrors; got != 8 {
		t.Fatalf("want summed lifetime 8 before delta, got %d", got)
	}
	c.applyLinkErrDelta(obs1.DeviceWindows)
	if got := obs1.DeviceWindows[0].LinkErrors; got != 0 {
		t.Fatalf("baseline scrape must report zero delta, got %d", got)
	}

	// Second scrape: counter grew by 4 ⇒ delta 4 ⇒ leg emitted.
	obs2, _ := parseDCGMExposition(strings.NewReader(dcgmLinkExposition(uuid, 7, 3, 2)), now, win, "lab-node")
	c.applyLinkErrDelta(obs2.DeviceWindows)
	if got := obs2.DeviceWindows[0].LinkErrors; got != 4 {
		t.Fatalf("want delta 4, got %d", got)
	}
	// Normalize it through a genuine DCGM observation → leg must appear @DCGM.
	w := mustNormalize(t, now, win, []Observation{obs2})
	ids := timelineIDs(w)
	if src, ok := ids["link.error."+uuid]; !ok {
		t.Fatalf("link.error leg not emitted on delta>0; ids=%v", ids)
	} else if src != gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM {
		t.Fatalf("link.error leg must be @DCGM, got %v", src)
	}

	// Third scrape: counter flat ⇒ zero delta ⇒ no leg.
	obs3, _ := parseDCGMExposition(strings.NewReader(dcgmLinkExposition(uuid, 7, 3, 2)), now, win, "lab-node")
	c.applyLinkErrDelta(obs3.DeviceWindows)
	w3 := mustNormalize(t, now, win, []Observation{obs3})
	if _, ok := timelineIDs(w3)["link.error."+uuid]; ok {
		t.Fatalf("flat counter must emit NO link.error leg")
	}
}

// TestDCGMLinkErrorAbsentNoLeg: a payload with NO link-error counters leaves
// LinkErrorsKnown false → no leg (degrade, never fabricate).
func TestDCGMLinkErrorAbsentNoLeg(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	obs, _ := parseDCGMExposition(strings.NewReader(
		`DCGM_FI_PROF_PIPE_TENSOR_ACTIVE{UUID="GPU-x",Hostname="n"} 0.5`+"\n"), now, win, "n")
	if len(obs.DeviceWindows) != 1 || obs.DeviceWindows[0].LinkErrorsKnown {
		t.Fatalf("absent link counters must leave LinkErrorsKnown false: %+v", obs.DeviceWindows)
	}
}

// --- PROC sysfs link-degraded leg (collectors_proc.go) ---

// writeSysfsDevice lays out a fake sysfs PCIe device dir with the given link
// width/speed attributes (empty value ⇒ file omitted).
func writeSysfsDevice(t *testing.T, root, addr, curW, maxW, curS, maxS string) {
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
	write("current_link_width", curW)
	write("max_link_width", maxW)
	write("current_link_speed", curS)
	write("max_link_speed", maxS)
}

// TestProcLinkDegradedWidthEmitsLeg: current width < max ⇒ link.degraded.width@PROC.
func TestProcLinkDegradedWidthEmitsLeg(t *testing.T) {
	now := fixedNow()
	root := t.TempDir()
	writeSysfsDevice(t, root, "0000:0a:00.0", "8\n", "16\n", "16.0 GT/s PCIe\n", "16.0 GT/s PCIe\n")

	obs, err := ProcLinkCollector{SysfsRoot: root, Node: "n"}.Collect(now, time.Minute)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if obs.Source != gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC {
		t.Fatalf("proc collector must be @PROC")
	}
	if len(obs.Timeline) != 1 {
		t.Fatalf("want 1 degraded leg, got %d: %+v", len(obs.Timeline), obs.Timeline)
	}
	te := obs.Timeline[0]
	if !strings.HasPrefix(te.GetSignalId(), "link.degraded.width.") {
		t.Fatalf("wrong signal id: %q", te.GetSignalId())
	}
	if te.GetSource() != gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC {
		t.Fatalf("leg must be @PROC")
	}
}

// TestProcLinkDegradedSpeedEmitsLeg: equal width but current speed < max ⇒
// link.degraded.speed@PROC.
func TestProcLinkDegradedSpeedEmitsLeg(t *testing.T) {
	now := fixedNow()
	root := t.TempDir()
	writeSysfsDevice(t, root, "0000:0b:00.0", "16\n", "16\n", "2.5 GT/s PCIe\n", "16.0 GT/s PCIe\n")
	obs, _ := ProcLinkCollector{SysfsRoot: root}.Collect(now, time.Minute)
	if len(obs.Timeline) != 1 || !strings.HasPrefix(obs.Timeline[0].GetSignalId(), "link.degraded.speed.") {
		t.Fatalf("want 1 speed-degraded leg, got %+v", obs.Timeline)
	}
}

// TestProcLinkEqualNoLeg: current == max (healthy link) ⇒ no leg.
func TestProcLinkEqualNoLeg(t *testing.T) {
	now := fixedNow()
	root := t.TempDir()
	writeSysfsDevice(t, root, "0000:0c:00.0", "16\n", "16\n", "16.0 GT/s PCIe\n", "16.0 GT/s PCIe\n")
	obs, _ := ProcLinkCollector{SysfsRoot: root}.Collect(now, time.Minute)
	if len(obs.Timeline) != 0 {
		t.Fatalf("healthy link must emit NO leg, got %+v", obs.Timeline)
	}
}

// TestProcLinkMissingRootDegrades: a non-existent sysfs root degrades cleanly
// (no leg, no error).
func TestProcLinkMissingRootDegrades(t *testing.T) {
	now := fixedNow()
	obs, err := ProcLinkCollector{SysfsRoot: filepath.Join(t.TempDir(), "does-not-exist")}.Collect(now, time.Minute)
	if err != nil {
		t.Fatalf("missing root must not error: %v", err)
	}
	if len(obs.Timeline) != 0 {
		t.Fatalf("missing root must emit NO leg")
	}
}

// --- End-to-end via the REAL rca engine (registry.NewDefaultEngine) ---

// linkErrDCGMCollector is a genuine DCGM collector emitting a link-error delta>0
// for a device (the honest DCGM leg, as a real exporter scrape would after delta).
type linkErrDCGMCollector struct {
	uuid  string
	delta uint64
}

func (linkErrDCGMCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM
}
func (c linkErrDCGMCollector) Collect(time.Time, time.Duration) (Observation, error) {
	return Observation{
		Source: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
		DeviceWindows: []DeviceWindow{
			{UUID: c.uuid, LinkErrors: c.delta, LinkErrorsKnown: true},
		},
	}, nil
}

// TestE2E_LinkDegradedFiresOnTwoSources: a DCGM link.error leg + an INDEPENDENT
// PROC link.degraded leg (two distinct sources) → the real engine FIRES
// LINK_DEGRADED.
func TestE2E_LinkDegradedFiresOnTwoSources(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-link-fire"
	root := t.TempDir()
	writeSysfsDevice(t, root, "0000:0a:00.0", "4\n", "16\n", "16.0 GT/s PCIe\n", "16.0 GT/s PCIe\n")

	cols := []Collector{
		linkErrDCGMCollector{uuid: uuid, delta: 3},
		ProcLinkCollector{SysfsRoot: root, Node: "n"},
	}
	obs := collectAll(t, cols, now, win)
	w := mustNormalize(t, now, win, obs)
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_LINK_DEGRADED {
		t.Fatalf("two-source link must FIRE LINK_DEGRADED, got %v", v.GetFaultClass())
	}
}

// TestE2E_LinkDegradedOneLegAbstains: only the DCGM link.error leg (no PROC
// corroborator) → ABSTAIN. And only the PROC leg (no DCGM) → ABSTAIN.
func TestE2E_LinkDegradedOneLegAbstains(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-link-solo"

	// DCGM leg only.
	obs := collectAll(t, []Collector{linkErrDCGMCollector{uuid: uuid, delta: 3}}, now, win)
	w := mustNormalize(t, now, win, obs)
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("DCGM-only link must ABSTAIN, got %v", v.GetFaultClass())
	}

	// PROC leg only.
	root := t.TempDir()
	writeSysfsDevice(t, root, "0000:0a:00.0", "4\n", "16\n", "", "")
	obs2 := collectAll(t, []Collector{ProcLinkCollector{SysfsRoot: root}}, now, win)
	w2 := mustNormalize(t, now, win, obs2)
	if v := evalVerdict(w2); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("PROC-only link must ABSTAIN, got %v", v.GetFaultClass())
	}
}

// TestE2E_LinkDegradedForgedOneSourceAbstains: both a link.error leg AND a
// link.degraded leg forged from a SINGLE DCGM collector → G1 + the gate's
// >=2-distinct-source veto collapse to ABSTAIN. The DCGM collector tries to stamp
// a PROC-claimed leg; the provenance guard drops it (source mismatch), and even a
// DCGM-stamped link.degraded is excluded by the playbook (DCGM is not a
// link.degraded source).
func TestE2E_LinkDegradedForgedOneSourceAbstains(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-link-forge"

	forge := forgeLinkCollector{
		uuid:  uuid,
		delta: 3,
		// A single DCGM collector tries to also mint the independent PROC leg.
		forgedLeg: &gpufleetv1.TimelineEntry{
			Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC, // claims PROC it does not own
			SignalId: "link.degraded.width.forged",
		},
	}
	obs := collectAll(t, []Collector{forge}, now, win)
	w := mustNormalize(t, now, win, obs)
	ids := timelineIDs(w)
	if _, ok := ids["link.degraded.width.forged"]; ok {
		t.Fatalf("forged PROC leg from a DCGM collector must be dropped by provenance guard; ids=%v", ids)
	}
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("forged single-source link must ABSTAIN, got %v", v.GetFaultClass())
	}
}

// forgeLinkCollector is a single DCGM collector that emits both a real DCGM
// link-error delta AND a pre-formed leg claiming a foreign (PROC) source — the
// forged-independence attack the provenance guard + G1 close.
type forgeLinkCollector struct {
	uuid      string
	delta     uint64
	forgedLeg *gpufleetv1.TimelineEntry
}

func (forgeLinkCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM
}
func (c forgeLinkCollector) Collect(time.Time, time.Duration) (Observation, error) {
	return Observation{
		Source:        gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
		DeviceWindows: []DeviceWindow{{UUID: c.uuid, LinkErrors: c.delta, LinkErrorsKnown: true}},
		Timeline:      []*gpufleetv1.TimelineEntry{c.forgedLeg},
	}, nil
}

// mustNormalize is a small helper to normalize and fail on error.
func mustNormalize(t *testing.T, now time.Time, win time.Duration, obs []Observation) *SignalWindow {
	t.Helper()
	w, err := Normalize("agent-link", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	return w
}
