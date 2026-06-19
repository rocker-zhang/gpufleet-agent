//go:build !gpu

package agent

import (
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// forgeOneCollector emits BOTH an ECC XID line AND a DCGM-shaped ECC double-bit
// counter for the same device under a SINGLE source — the forged-independence
// attack G1 closes (one physical collector trying to mint two "independent" legs).
type forgeOneCollector struct {
	src  gpufleetv1.SignalSource
	xids []*gpufleetv1.XidEvent
	dws  []DeviceWindow
}

func (c forgeOneCollector) Source() gpufleetv1.SignalSource { return c.src }
func (c forgeOneCollector) Collect(time.Time, time.Duration) (Observation, error) {
	return Observation{Source: c.src, XidEvents: c.xids, DeviceWindows: c.dws}, nil
}

// TestG1_SingleCollectorCannotForgeECCIndependence: a single collector emitting
// both the ECC-XID line and an ECC double-bit counter must NOT produce two
// different-source legs. The ECC-counter leg is only honored from a DCGM
// collector, so here only the one real DMESG_XID leg survives → the gate ABSTAINs
// (before G1 this FIRED ECC_UNCORRECTABLE conf 0.95 from one physical source).
func TestG1_SingleCollectorCannotForgeECCIndependence(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-mock-0009"

	forge := forgeOneCollector{
		src: gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
		xids: []*gpufleetv1.XidEvent{
			{Ts: timestamppb.New(now), DeviceUuid: uuid, Xid: 94, RawMessage: "Xid 94 ecc", Severity: "err"},
		},
		dws: []DeviceWindow{{UUID: uuid, ECCDoubleBitErrs: 1, ECCDoubleBitKnown: true}},
	}
	obs := collectAll(t, []Collector{forge}, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	ids := timelineIDs(w)
	if src, ok := ids["ecc.dbe."+uuid]; ok {
		t.Fatalf("forged ecc.dbe leg minted from a non-DCGM collector (src=%v) — G1 regression", src)
	}
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Fatalf("single-collector ECC must ABSTAIN, got %v", v.GetFaultClass())
	}
}

// TestG1_TwoGenuineCollectorsECCFires: the SAME facts, split across two genuine
// collectors (DMESG_XID XID line + DCGM counter), are real independent sources →
// ECC_UNCORRECTABLE FIRES. Proves G1 is precise: it blocks forgery, not real 2-source.
func TestG1_TwoGenuineCollectorsECCFires(t *testing.T) {
	now := fixedNow()
	win := time.Minute
	uuid := "GPU-mock-0001" // a device the DCGM mock emits, so InjectECCDBE yields a DeviceWindow

	cols := []Collector{
		MockDCGMCollector{Node: "n", InjectECCDBE: map[string]uint64{uuid: 2}},
		MockDmesgCollector{Node: "n", Inject: []*gpufleetv1.XidEvent{
			{Ts: timestamppb.New(now), DeviceUuid: uuid, Xid: 94, RawMessage: "Xid 94 ecc", Severity: "err"},
		}},
	}
	obs := collectAll(t, cols, now, win)
	w, err := Normalize("agent-x", now, win, obs)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if v := evalVerdict(w); v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ECC_UNCORRECTABLE {
		t.Fatalf("two-source ECC must FIRE ECC_UNCORRECTABLE, got %v", v.GetFaultClass())
	}
}
