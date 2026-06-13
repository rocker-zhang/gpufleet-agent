package agent

import (
	"fmt"
	"sort"
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

		for _, m := range o.Mappings {
			// Last writer wins only if a prior source left job empty; otherwise
			// keep the first non-empty (deterministic given fixed source order).
			if prev, ok := mappingByDevice[m.DeviceUuid]; !ok || prev.JobId == "" {
				mappingByDevice[m.DeviceUuid] = m
			}
		}

		for _, dw := range o.DeviceWindows {
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

	for _, u := range uuids {
		dw := merged[u].dw
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

	// Stamp a non-adjudicating timeline entry per source for citation ordering.
	for _, s := range sources {
		pack.Timeline = append(pack.Timeline, &gpufleetv1.TimelineEntry{
			Ts:     timestamppb.New(now),
			Source: s,
			Label:  fmt.Sprintf("%s window", sourceShort(s)),
		})
	}

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
