// Package agent is the read-only, off-critical-path collector/sidecar for
// gpufleet. It reads existing telemetry sources (DCGM-exporter / Prometheus /
// dmesg / NCCL) and normalizes them into a gpufleet.v1 SignalSchema window
// (the proto EvidencePack). It NEVER controls, orchestrates, throttles,
// checkpoints, or kills GPUs/jobs and is never in a job-execution path
// (RULES §A). A gpufleet failure must never affect a customer job.
//
// The default build uses mock sources (no GPU required, //go:build !gpu). The
// real NVML-backed reader is isolated behind the `gpu` build tag, so the
// standard CI build and shipped binaries are CPU-only and need no cgo.
//
// Boundary types are the REAL generated proto contracts, vendored read-only at
// proto tag proto/v0.1.0 (module github.com/rocker-zhang/gpufleet-proto/gen/go).
// This package is a real consumer of those gen types, not a hand-rolled mirror.
package agent

import (
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// Collector is the read-only signal source interface. Each collector observes
// one provenance class (DCGM, Prometheus, dmesg/XID, ...) and returns a raw,
// source-tagged Observation for a bounded window. Implementations MUST be
// side-effect-free with respect to the GPU and the running job: they read and
// emit, never write back, control, schedule, throttle, or kill (RULES §A).
type Collector interface {
	// Source reports the provenance class of every signal this collector emits.
	// Independence for the >=2-signal gate is judged on this source, so a
	// collector returns exactly one SignalSource.
	Source() gpufleetv1.SignalSource

	// Collect returns the raw observation for the window ending at `now`. It is
	// read-only and must never fabricate fields: a field the source did not
	// provide is left unset and marked missing by the normalizer, never invented.
	Collect(now time.Time, window time.Duration) (Observation, error)
}

// Observation is the raw, source-tagged output of one Collector for one window.
// It is intentionally close to the gpufleet.v1 wire shapes (it carries slices of
// the generated proto sub-messages) so the normalizer merges sources without
// re-interpreting vendor semantics. Every observation is stamped with its
// SignalSource for provenance and independence accounting.
type Observation struct {
	// Source is the provenance class of this observation (copied from the
	// collector). Stamped onto every normalized signal that derives from it.
	Source gpufleetv1.SignalSource

	// Mappings are device<->job ownership edges this source could resolve. A
	// scheduler/Prometheus source typically supplies these; a dmesg source
	// usually supplies none (and the normalizer marks the job scope missing).
	Mappings []*gpufleetv1.DeviceJobMapping

	// DcgmSeries are time-series field samples (e.g. PIPE_TENSOR_ACTIVE) keyed by
	// device. Carried raw; MFU/cost semantics are derived downstream in semantics.
	DcgmSeries []*gpufleetv1.DcgmFieldSeries

	// XidEvents are discrete kernel/XID fault events observed in the window.
	XidEvents []*gpufleetv1.XidEvent

	// NcclEvents are collective-communication observations (timeouts, slow
	// all-reduce, rank desync) parsed from NCCL logs in the window. Carried raw;
	// they are wire-level observations, never a fault verdict (RULES §B).
	NcclEvents []*gpufleetv1.NcclEvent

	// DeviceWindows are per-device measured aggregates the normalizer feeds to
	// the standalone cost wedge. They are kept separate from DcgmSeries because
	// the cost wedge consumes scalar window aggregates, not raw sample streams.
	DeviceWindows []DeviceWindow

	// Provenance is free-form, non-adjudicating collection metadata (hostnames,
	// exporter versions, scrape intervals). Keys are collector-defined.
	Provenance map[string]string
}

// DeviceWindow is one device's scalar measured aggregates over the window, as
// read by a source. Fields the source did not provide are flagged via the
// *Known booleans so the normalizer degrades (marks missing) instead of
// fabricating a value. This is the determinism/provenance contract (RULES §B):
// missing fields are MARKED, never invented.
type DeviceWindow struct {
	UUID  string
	Node  string
	Model string

	WindowSeconds float64

	// AchievedFLOPs is total floating-point ops in the window. Valid only when
	// AchievedFLOPsKnown; otherwise MFU/cost cannot be computed and is degraded.
	AchievedFLOPs      float64
	AchievedFLOPsKnown bool

	// TensorActiveSecs is seconds tensor pipes were active. Valid only when
	// TensorActiveKnown.
	TensorActiveSecs  float64
	TensorActiveKnown bool

	// PeakFLOPS is the device's advertised peak (for MFU). Valid only when
	// PeakFLOPSKnown; unknown peak ⇒ MFU cannot be computed (degraded), per the
	// proto contract ("treat unknown as cannot compute MFU, not zero").
	PeakFLOPS      float64
	PeakFLOPSKnown bool

	// CostPerHour is the $/hour rate for cost attribution. Valid only when
	// CostKnown; unknown cost ⇒ the wedge reports unpriced, never a fabricated $.
	CostPerHour float64
	CostKnown   bool
}
