package agent

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	semantics "github.com/rocker-zhang/gpufleet-semantics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// contractVersion is the gpufleet.v1 schema version this agent produces against.
const contractVersion = "v1"

// DegradeMark records one missing-field degradation: a fact the agent could NOT
// compute because a source did not supply an input. Degrade-never-fabricate is
// the determinism/provenance contract (RULES §B): the agent marks the gap
// rather than inventing a value.
type DegradeMark struct {
	DeviceUUID string // device the gap applies to
	Field      string // normalized field that could not be computed (e.g. "mfu")
	Reason     string // why (which input was missing)
}

// SignalWindow is the normalized, source-merged result for one collection
// window: the proto EvidencePack (the gpufleet.v1 SignalSchema window), the
// resolved device->job grouping (via semantics.ResolveMapping), the per-device
// measured aggregates fed to the cost wedge, and the degradation marks. It is
// the single read-only snapshot the local API serves.
type SignalWindow struct {
	// Pack is the normalized gpufleet.v1 EvidencePack — the real generated proto
	// type, vendored at proto/v0.1.0. This is the evidence window shape.
	Pack *gpufleetv1.EvidencePack

	// Jobs is the deterministic device->job grouping resolved by semantics.
	Jobs []semantics.JobDevices

	// Samples are the per-device measured aggregates (only devices whose MFU
	// inputs are fully known), keyed for the cost wedge.
	Samples map[string]semantics.DeviceSample
	// Specs are the per-device peak/cost specs paired with Samples.
	Specs map[string]semantics.DeviceSpec
	// DeviceNode/DeviceModel preserve identity for devices in Samples.
	devices map[string]semantics.Device

	// Degraded lists every missing-field degradation in this window.
	Degraded []DegradeMark

	// Sources lists the SignalSources that contributed, for provenance.
	Sources []gpufleetv1.SignalSource

	WindowStart time.Time
	WindowEnd   time.Time
}

// mergedDevice accumulates one device's fields across sources, tracking which
// inputs are known so the normalizer can degrade rather than fabricate.
type mergedDevice struct {
	dw DeviceWindow
}

// Normalize merges raw, source-tagged observations into one SignalWindow. It is
// deterministic: device ordering is by UUID, jobs by ID. Cross-source field
// completion is additive (a peak/cost supplied by Prometheus fills a gap DCGM
// left); a field NO source supplied is marked degraded, never invented.
func Normalize(agentID string, now time.Time, window time.Duration, obs []Observation) (*SignalWindow, error) {
	start := now.Add(-window)
	pack := &gpufleetv1.EvidencePack{
		ContractVersion: contractVersion,
		AgentId:         agentID,
		WindowStart:     timestamppb.New(start),
		WindowEnd:       timestamppb.New(now),
		Provenance:      map[string]string{},
	}

	merged := map[string]*mergedDevice{}
	mappingByDevice := map[string]*gpufleetv1.DeviceJobMapping{}
	var sources []gpufleetv1.SignalSource
	seenSource := map[gpufleetv1.SignalSource]bool{}
	// injectedTimeline holds source-observed, pre-formed timeline signals carried
	// verbatim from collectors (provenance-validated above), appended to the pack
	// timeline after the derived per-fault legs.
	var injectedTimeline []*gpufleetv1.TimelineEntry
	// Source-attributed inputs for the DERIVED gate legs (G1): only facts observed
	// by a collector that legitimately speaks the leg's source feed faultTimeline,
	// so one physical collector can never mint two different-source legs.
	var xidDmesg []*gpufleetv1.XidEvent
	var ncclReal []*gpufleetv1.NcclEvent
	eccDCGMSeen := map[string]bool{}
	eccPromSeen := map[string]bool{}
	linkErrDCGMSeen := map[string]bool{}

	for _, o := range obs {
		if !seenSource[o.Source] {
			seenSource[o.Source] = true
			sources = append(sources, o.Source)
		}
		// Provenance: namespace each source's keys so they never collide.
		for k, v := range o.Provenance {
			pack.Provenance[fmt.Sprintf("%s.%s", sourceShort(o.Source), k)] = v
		}
		pack.DcgmSeries = append(pack.DcgmSeries, o.DcgmSeries...)
		pack.XidEvents = append(pack.XidEvents, o.XidEvents...)
		pack.NcclEvents = append(pack.NcclEvents, o.NcclEvents...)
		// Source-attributed inputs for the DERIVED gate legs (G1 provenance
		// integrity, RULES §B). A derived leg may only be minted from a fact
		// observed by a collector that legitimately speaks that leg's source — so a
		// single physical collector can NEVER mint two different-source legs and
		// forge the gate's independence axis (the same property the pre-formed-leg
		// guard above enforces for o.Timeline). XID legs only from a DMESG_XID
		// collector; NCCL legs only from a genuine NCCL collector; the ECC-counter
		// leg only from a DCGM collector (tracked in the DeviceWindows loop below).
		// Two legs from one collector therefore necessarily share that collector's
		// Source, so the engine's >=2-distinct-source veto rejects them → ABSTAIN.
		if o.Source == gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID {
			xidDmesg = append(xidDmesg, o.XidEvents...)
		}
		if o.Source == gpufleetv1.SignalSource_SIGNAL_SOURCE_NCCL {
			ncclReal = append(ncclReal, o.NcclEvents...)
		}
		// Carry pre-formed, source-observed timeline signals verbatim. Provenance
		// integrity (RULES §B/§F): only entries whose Source matches the observation's
		// Source are accepted, so a collector cannot stamp a leg onto a source it does
		// not speak for (which would forge the gate's independence axis). A nil/empty
		// or mismatched Source is normalized to the observation's own Source.
		for _, te := range o.Timeline {
			if te == nil {
				continue
			}
			if te.Source == gpufleetv1.SignalSource_SIGNAL_SOURCE_UNSPECIFIED {
				te.Source = o.Source
			}
			if te.Source != o.Source {
				continue // drop a leg claiming a source this collector does not own.
			}
			injectedTimeline = append(injectedTimeline, te)
		}

		for _, m := range o.Mappings {
			// Last writer wins only if a prior source left job empty; otherwise
			// keep the first non-empty (deterministic given fixed source order).
			if prev, ok := mappingByDevice[m.DeviceUuid]; !ok || prev.JobId == "" {
				mappingByDevice[m.DeviceUuid] = m
			}
		}

		for _, dw := range o.DeviceWindows {
			// ECC-counter leg provenance (G1): only a genuine metrics collector's
			// double-bit delta>0 may later mint the `ecc.dbe.<uuid>` leg, attributed to
			// the OBSERVING collector's source. A DCGM-exporter scrape mints it @DCGM;
			// a Prometheus-primary node (which reads the SAME counter via an instant
			// `increase()` query, a genuinely independent metrics path) mints it
			// @PROMETHEUS. Either is an honest, source-faithful counter leg; the
			// eccuncorrectable playbook accepts both for the counter leg (its
			// independent corroborator is the kernel dmesg ECC Xid). A single physical
			// collector still cannot mint two different-source legs (it has one Source).
			if o.Source == gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM &&
				dw.ECCDoubleBitKnown && dw.ECCDoubleBitErrs > 0 {
				eccDCGMSeen[dw.UUID] = true
			}
			if o.Source == gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS &&
				dw.ECCDoubleBitKnown && dw.ECCDoubleBitErrs > 0 {
				eccPromSeen[dw.UUID] = true
			}
			// link.error leg provenance (G1): only a genuine DCGM collector's
			// link-error delta>0 may later mint the `link.error.<uuid>`@DCGM leg.
			if o.Source == gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM &&
				dw.LinkErrorsKnown && dw.LinkErrors > 0 {
				linkErrDCGMSeen[dw.UUID] = true
			}
			md, ok := merged[dw.UUID]
			if !ok {
				md = &mergedDevice{dw: DeviceWindow{UUID: dw.UUID}}
				merged[dw.UUID] = md
			}
			mergeDeviceWindow(&md.dw, dw)
		}
	}

	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })

	out := &SignalWindow{
		Pack:        pack,
		Samples:     map[string]semantics.DeviceSample{},
		Specs:       map[string]semantics.DeviceSpec{},
		devices:     map[string]semantics.Device{},
		Sources:     sources,
		WindowStart: start,
		WindowEnd:   now,
	}

	// Deterministic device iteration.
	uuids := make([]string, 0, len(merged))
	for u := range merged {
		uuids = append(uuids, u)
	}
	sort.Strings(uuids)

	// eccDBEDevices collects, in sorted-UUID order, the devices whose uncorrectable
	// (double-bit) ECC counter delta was genuinely observed as >0, each tagged with
	// the SOURCE of the collector that observed it (DCGM exporter scrape ⇒ @DCGM;
	// Prometheus-primary increase() query ⇒ @PROMETHEUS). This is the counter leg of
	// the ECC-uncorrectable gate signature, emitted below as `ecc.dbe.<uuid>` @ that
	// source. A device with no ECC reading (ECCDoubleBitKnown false) or a zero delta
	// contributes nothing — degrade, never fabricate.
	var eccDBEDevices []eccDBEDevice
	// linkErrDevices collects, in sorted-UUID order, the devices whose DCGM
	// link-error counter delta was genuinely observed as >0 — the DCGM leg of the
	// LINK_DEGRADED gate signature, emitted below as `link.error.<uuid>`@DCGM. A
	// device with no link-error reading (LinkErrorsKnown false) or a zero delta
	// contributes nothing — degrade, never fabricate.
	var linkErrDevices []string

	for _, u := range uuids {
		dw := merged[u].dw
		// Emit the ECC-counter leg only when the double-bit delta was genuinely
		// observed by a metrics collector (G1): the *Seen maps gate out a counter that
		// rode in on a source that does not own it. The leg is attributed to the
		// observing source. DCGM is preferred when both saw it (the lower-latency
		// local scrape); a Prometheus-primary node with no DCGM scrape mints it
		// @PROMETHEUS. Exactly one counter leg per device (one Source), so a single
		// collector can never mint two different-source legs.
		switch {
		case dw.ECCDoubleBitKnown && dw.ECCDoubleBitErrs > 0 && eccDCGMSeen[u]:
			eccDBEDevices = append(eccDBEDevices, eccDBEDevice{
				uuid: u, source: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
			})
		case dw.ECCDoubleBitKnown && dw.ECCDoubleBitErrs > 0 && eccPromSeen[u]:
			eccDBEDevices = append(eccDBEDevices, eccDBEDevice{
				uuid: u, source: gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS,
			})
		}
		// Emit the link-error leg only when the delta was genuinely observed by a
		// DCGM collector (G1): linkErrDCGMSeen gates out a counter that rode in on a
		// non-DCGM observation, so it cannot forge the DCGM leg.
		if dw.LinkErrorsKnown && dw.LinkErrors > 0 && linkErrDCGMSeen[u] {
			linkErrDevices = append(linkErrDevices, u)
		}
		dev := semantics.Device{UUID: dw.UUID, Node: dw.Node, Model: dw.Model}
		out.devices[u] = dev

		// Build the device->job mapping for the proto pack. If no source resolved
		// a job, the scope is marked missing (degraded) and job_id left empty.
		m := mappingByDevice[u]
		if m == nil {
			m = &gpufleetv1.DeviceJobMapping{DeviceUuid: u, Node: dw.Node}
			out.Degraded = append(out.Degraded, DegradeMark{
				DeviceUUID: u, Field: "job_id",
				Reason: "no scheduler/Prometheus source supplied a device->job mapping",
			})
		}
		// Reflect known peak/cost onto the mapping (proto: zero means unknown).
		if dw.PeakFLOPSKnown {
			m.PeakTflops = dw.PeakFLOPS / 1e12
		}
		if dw.CostKnown {
			m.CostRateUsdPerHour = dw.CostPerHour
		}
		pack.Mappings = append(pack.Mappings, m)

		// Determine whether the cost-wedge inputs are fully known; degrade per
		// missing input instead of fabricating.
		canMFU := true
		if dw.WindowSeconds <= 0 {
			canMFU = false
			out.Degraded = append(out.Degraded, DegradeMark{u, "window_seconds", "no source supplied a positive measurement window"})
		}
		if !dw.AchievedFLOPsKnown {
			canMFU = false
			out.Degraded = append(out.Degraded, DegradeMark{u, "achieved_flops", "no source supplied achieved FLOPs"})
		}
		if !dw.PeakFLOPSKnown {
			canMFU = false
			out.Degraded = append(out.Degraded, DegradeMark{u, "mfu", "peak FLOPS unknown from every source (cannot compute MFU, not zero)"})
		}
		if !dw.TensorActiveKnown {
			// Tensor-active feeds the LOW_UTILIZATION corroboration; mark but it
			// does not by itself block MFU.
			out.Degraded = append(out.Degraded, DegradeMark{u, "tensor_active", "no source supplied tensor-active seconds"})
		}
		if !dw.CostKnown {
			// Unpriced device: the wedge reports unpriced (WastedUSD=0), never a
			// fabricated dollar amount. Marked for transparency.
			out.Degraded = append(out.Degraded, DegradeMark{u, "cost", "no source supplied a $/hour rate (wedge reports unpriced)"})
		}

		if canMFU {
			out.Samples[u] = semantics.DeviceSample{
				Device:           dev,
				WindowSeconds:    dw.WindowSeconds,
				AchievedFLOPs:    dw.AchievedFLOPs,
				TensorActiveSecs: dw.TensorActiveSecs,
			}
			out.Specs[u] = semantics.DeviceSpec{
				PeakFLOPS:   dw.PeakFLOPS,
				CostPerHour: dw.CostPerHour, // 0 when unknown ⇒ unpriced, never faked
			}
		}
	}

	// Resolve device->job via semantics (deterministic grouping). Only devices
	// with a known job participate; unmapped devices stay job-less in the pack.
	var edges []semantics.DeviceJob
	for _, u := range uuids {
		m := mappingByDevice[u]
		if m == nil || m.JobId == "" {
			continue
		}
		edges = append(edges, semantics.DeviceJob{
			Device: out.devices[u],
			Job:    semantics.Job{ID: m.JobId},
		})
	}
	jobs, err := semantics.ResolveMapping(edges)
	if err != nil {
		return nil, fmt.Errorf("agent: resolve device->job mapping: %w", err)
	}
	out.Jobs = jobs

	// Stamp a non-adjudicating timeline entry per source for citation ordering
	// (kept for provenance/back-compat; these carry NO signal_id, so the rca gate
	// skips them — they never count as a gate leg).
	for _, s := range sources {
		pack.Timeline = append(pack.Timeline, &gpufleetv1.TimelineEntry{
			Ts:     timestamppb.New(now),
			Source: s,
			Label:  fmt.Sprintf("%s window", sourceShort(s)),
		})
	}

	// Emit per-fault, device-attributed timeline signal_ids the rca gate matches
	// on. EVERY id below traces to a fact the agent GENUINELY collected this
	// window — an XID line in pack.XidEvents, an NCCL OP_TIMEOUT in
	// pack.NcclEvents, or a DCGM ECC double-bit counter delta>0 — so the gate only
	// ever sees real, grounded evidence (RULES §B: degrade, never fabricate; we do
	// NOT synthesize a corroborator the agent does not collect). The id prefixes +
	// Sources match the public rca playbook conventions EXACTLY. Deterministic
	// (sorted) emission for stable verdicts.
	pack.Timeline = append(pack.Timeline, faultTimeline(xidDmesg, ncclReal, eccDBEDevices, linkErrDevices, now)...)
	// Append source-observed, pre-formed legs carried verbatim from collectors.
	pack.Timeline = append(pack.Timeline, injectedTimeline...)

	pack.Provenance["agent.contract_version"] = contractVersion
	pack.Provenance["agent.window_seconds"] = fmt.Sprintf("%.0f", window.Seconds())
	pack.Provenance["agent.degraded_marks"] = fmt.Sprintf("%d", len(out.Degraded))

	return out, nil
}

// mergeDeviceWindow folds src into dst additively: a known field on src fills a
// gap on dst but never overwrites an already-known value with an unknown one.
func mergeDeviceWindow(dst *DeviceWindow, src DeviceWindow) {
	if src.Node != "" {
		dst.Node = src.Node
	}
	if src.Model != "" {
		dst.Model = src.Model
	}
	if src.WindowSeconds > 0 {
		dst.WindowSeconds = src.WindowSeconds
	}
	if src.AchievedFLOPsKnown && !dst.AchievedFLOPsKnown {
		dst.AchievedFLOPs, dst.AchievedFLOPsKnown = src.AchievedFLOPs, true
	}
	if src.TensorActiveKnown && !dst.TensorActiveKnown {
		dst.TensorActiveSecs, dst.TensorActiveKnown = src.TensorActiveSecs, true
	}
	if src.PeakFLOPSKnown && !dst.PeakFLOPSKnown {
		dst.PeakFLOPS, dst.PeakFLOPSKnown = src.PeakFLOPS, true
	}
	if src.CostKnown && !dst.CostKnown {
		dst.CostPerHour, dst.CostKnown = src.CostPerHour, true
	}
	if src.ECCDoubleBitKnown && !dst.ECCDoubleBitKnown {
		dst.ECCDoubleBitErrs, dst.ECCDoubleBitKnown = src.ECCDoubleBitErrs, true
	}
	if src.LinkErrorsKnown && !dst.LinkErrorsKnown {
		dst.LinkErrors, dst.LinkErrorsKnown = src.LinkErrors, true
	}
}

func sourceShort(s gpufleetv1.SignalSource) string {
	switch s {
	case gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM:
		return "dcgm"
	case gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID:
		return "dmesg"
	case gpufleetv1.SignalSource_SIGNAL_SOURCE_NCCL:
		return "nccl"
	case gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS:
		return "prometheus"
	case gpufleetv1.SignalSource_SIGNAL_SOURCE_SCHEDULER:
		return "scheduler"
	case gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC:
		return "proc"
	default:
		return "unspecified"
	}
}

// eccDBEDevice is a device whose uncorrectable (double-bit) ECC counter delta was
// genuinely observed as >0, tagged with the SOURCE of the observing metrics
// collector (DCGM exporter scrape ⇒ DCGM; Prometheus increase() query ⇒
// PROMETHEUS). The counter leg `ecc.dbe.<uuid>` is emitted @ this source.
type eccDBEDevice struct {
	uuid   string
	source gpufleetv1.SignalSource
}

// eccXids is the public set of NVIDIA XID numbers that denote an uncorrectable
// (double-bit) GPU memory ECC error. PUBLIC semantics only (RULES §F): these are
// the documented public ECC XIDs, not externally-sourced or secret codes.
var eccXids = map[uint32]bool{48: true, 63: true, 64: true, 94: true, 95: true}

// faultTimeline derives the per-fault, device-attributed timeline signal_ids the
// rca gate matches on, from data the agent GENUINELY collected this window. It is
// the single honesty chokepoint (RULES §B): each emitted id traces to a real
// observed fact — an XID line, an NCCL OP_TIMEOUT, or a DCGM ECC double-bit delta
// — and NO corroborator the agent does not collect is ever synthesized.
//
// PROVENANCE (G1): the inputs are SOURCE-ATTRIBUTED by the caller — `xids` only
// from a DMESG_XID collector, `nccls` only from a genuine NCCL collector, and
// `eccDBEDevices` only from a DCGM collector. A leg's stamped Source therefore
// always equals the source of the collector that observed it, so a single
// physical collector cannot mint two different-source legs (forged independence);
// any two legs it produces share its source and the engine's >=2-distinct-source
// veto collapses them to ABSTAIN. With the current real collectors THREE faults
// have two independent real legs and can FIRE:
//   - ECC (ECC-XID@DMESG_XID + ECC-counter@{DCGM exporter scrape | PROMETHEUS
//     increase() query}). The counter leg is source-faithful to whichever metrics
//     collector observed it, so ECC fires on a DCGM-fallback OR a Prometheus-
//     primary node.
//   - LINK_DEGRADED (link.error@DCGM + link.degraded.{width,speed}@PROC from the
//     sysfs PCIe-link collector).
//   - XID79 (dmesg.xid79@DMESG_XID + device.lost@PROC from the sysfs collector,
//     which observes an NVIDIA GPU whose PCIe link is fully down — fallen off the
//     bus).
//
// An NCCL timeout still emits only its single real leg (nccl.timeout@NCCL from the
// dedicated NCCL-log collector) and the gate correctly ABSTAINs: its second source
// — an INDEPENDENT collective.stall witness (scheduler/DCGM/PROC) — has no genuine,
// non-speculative collector, so we emit ONLY the one real leg and never fabricate
// the corroborator (RULES §B).
//
// Output is deterministic: XID events in pack order (already sorted by the
// collector), then NCCL events in pack order, then ECC-counter ids in sorted-UUID
// order. The id prefixes + Sources match the public rca playbook conventions
// EXACTLY (xid79/eccuncorrectable/nccltimeout).
func faultTimeline(xids []*gpufleetv1.XidEvent, nccls []*gpufleetv1.NcclEvent, eccDBEDevices []eccDBEDevice, linkErrDevices []string, now time.Time) []*gpufleetv1.TimelineEntry {
	var out []*gpufleetv1.TimelineEntry

	// (1) XID-derived dmesg legs @ DMESG_XID.
	for _, e := range xids {
		if e == nil {
			continue
		}
		var id, label string
		switch {
		case e.GetXid() == 79:
			id = "dmesg.xid79." + xidDevToken(e)
			label = "NVRM Xid 79 (GPU fallen off the bus) on " + xidDevToken(e)
		case eccXids[e.GetXid()]:
			id = fmt.Sprintf("dmesg.xid.ecc.%d.%s", e.GetXid(), xidDevToken(e))
			label = fmt.Sprintf("NVRM Xid %d (uncorrectable ECC) on %s", e.GetXid(), xidDevToken(e))
		default:
			continue // not a fault class the open gate adjudicates; carry no leg.
		}
		ts := e.GetTs()
		if ts == nil {
			ts = timestamppb.New(now)
		}
		out = append(out, &gpufleetv1.TimelineEntry{
			Ts:       ts,
			Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
			SignalId: id,
			Label:    label,
		})
	}

	// (2) NCCL OP_TIMEOUT legs @ NCCL.
	for _, e := range nccls {
		if e == nil || e.GetKind() != gpufleetv1.NcclEventKind_NCCL_EVENT_KIND_OP_TIMEOUT {
			continue
		}
		id := "nccl.timeout." + ncclScopeToken(e)
		ts := e.GetTs()
		if ts == nil {
			ts = timestamppb.New(now)
		}
		out = append(out, &gpufleetv1.TimelineEntry{
			Ts:       ts,
			Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_NCCL,
			SignalId: id,
			Label:    "NCCL collective timeout (" + ncclScopeToken(e) + ")",
		})
	}

	// (3) ECC double-bit counter legs (delta>0, sorted by UUID), attributed to the
	// observing metrics source: a DCGM-exporter scrape ⇒ @DCGM, a Prometheus-primary
	// increase() query ⇒ @PROMETHEUS. Both are honest, source-faithful counter legs;
	// the eccuncorrectable playbook accepts either for the counter leg.
	for _, d := range eccDBEDevices {
		out = append(out, &gpufleetv1.TimelineEntry{
			Ts:       timestamppb.New(now),
			Source:   d.source,
			SignalId: "ecc.dbe." + d.uuid,
			Label: "uncorrectable (double-bit) ECC counter delta on " + d.uuid +
				" (" + sourceShort(d.source) + ")",
		})
	}

	// (4) DCGM link-error counter legs @ DCGM (NVLink CRC/replay + PCIe replay
	// delta>0, sorted by UUID). This is the DCGM leg of the LINK_DEGRADED gate; it
	// FIRES only when corroborated by an INDEPENDENT non-DCGM `link.degraded.*`
	// witness (a PROC sysfs link-width downgrade), otherwise the gate ABSTAINs.
	for _, u := range linkErrDevices {
		out = append(out, &gpufleetv1.TimelineEntry{
			Ts:       timestamppb.New(now),
			Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
			SignalId: "link.error." + u,
			Label:    "DCGM NVLink/PCIe link-error counter delta on " + u,
		})
	}

	return out
}

// xidDevToken returns a stable, attributable device token for an XID event: the
// device UUID when present, else the PCI/bus identity sanitized out of the raw
// kernel line, else "node" (a node-scoped XID with no device attribution). The
// token never contains a '.', so it cannot break the dotted signal_id prefix the
// rca playbooks match.
func xidDevToken(e *gpufleetv1.XidEvent) string {
	if u := e.GetDeviceUuid(); u != "" {
		return sanitizeToken(u)
	}
	return "node"
}

// ncclScopeToken returns a stable scope token for an NCCL event: the
// communicator id when present, else the rank, else "unknown". Sanitized so it
// never breaks the dotted signal_id prefix.
func ncclScopeToken(e *gpufleetv1.NcclEvent) string {
	if c := e.GetCommunicatorId(); c != "" {
		return sanitizeToken(c)
	}
	if e.GetRank() != 0 {
		return "rank" + strconv.FormatUint(uint64(e.GetRank()), 10)
	}
	return "unknown"
}

// sanitizeToken replaces dots (the signal_id segment separator) with '-' so a
// device/scope identity embedded into an id cannot split the prefix the rca
// playbooks prefix-match on.
func sanitizeToken(s string) string {
	return strings.ReplaceAll(s, ".", "-")
}
