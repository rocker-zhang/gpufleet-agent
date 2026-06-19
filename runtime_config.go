package agent

import (
	"bytes"
	"io"
	"os"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file is the build-AGNOSTIC runtime endpoint wiring (TASK-0037): it turns
// a flat RuntimeConfig (the `agent -serve --prometheus-url … --dcgm-exporter-url
// …` flags / env) into the daemon's collector list, choosing between the MOCK
// DefaultCollectors and the REAL HTTP MetricsChain (Prometheus-first → DCGM
// fallback) + log/event collector.
//
// It lives in the package (not in cmd/agent) so it is unit-testable without a
// process: a test points RuntimeConfig at httptest servers and asserts the
// REAL chain was wired (not mock). The real collectors are pure HTTP scrapers
// (collectors_prometheus.go / collectors_dcgm.go) and compile in the DEFAULT
// !gpu build — so real-endpoint collection needs NO NVML / no -tags gpu (RULES
// §H, module CLAUDE.md). It is strictly read-only: it only constructs read
// collectors + config, zero write-back (RULES §A). An unreachable/garbage
// endpoint is NOT validated/dialed here; it surfaces at Collect time and the
// existing degradation chain marks+degrades — it never crashes the daemon.

// CollectorMode selects which collector set the daemon runs.
type CollectorMode string

const (
	// CollectorModeAuto uses the REAL collectors when any real endpoint
	// (Prometheus or DCGM-exporter) is configured, else the MOCK collectors. This
	// is the default and keeps demo1/no-endpoint behavior unchanged.
	CollectorModeAuto CollectorMode = "auto"
	// CollectorModeMock forces the MOCK DefaultCollectors regardless of endpoints.
	CollectorModeMock CollectorMode = "mock"
	// CollectorModeReal forces the REAL HTTP MetricsChain + log collector. With no
	// endpoint configured the chain simply has no source to answer and degrades
	// (never crashes) — useful to assert the real path explicitly.
	CollectorModeReal CollectorMode = "real"
)

// RuntimeConfig is the flat, flag/env-derived runtime wiring for the daemon's
// collectors. The zero value (no endpoints, Mode "" ⇒ auto) reproduces the
// historical mock behavior, so an agent invoked with NO endpoint flags is
// byte-for-byte backward compatible (demo1 regression green).
type RuntimeConfig struct {
	// Node is the node name stamped on emitted evidence (the existing -node flag).
	Node string
	// PrometheusURL is the EXISTING Prometheus server root (e.g.
	// "http://prometheus:9090"). Empty ⇒ no Prometheus primary. Read-only: it is
	// queried via the instant-query HTTP API, zero new scrape load.
	PrometheusURL string
	// DCGMExporterURL is the local DCGM-exporter /metrics endpoint (e.g.
	// "http://localhost:9400/metrics"). Empty ⇒ no DCGM fallback. Read-only GET.
	DCGMExporterURL string
	// Mode selects auto|mock|real. Empty ⇒ auto.
	Mode CollectorMode

	// Queries overrides the PromQL expressions read from Prometheus. Zero value
	// uses DefaultPromQueries (the common DCGM-exporter labeling). Only consulted
	// when PrometheusURL is set. An operator aligns these to the real
	// dcgm-exporter schema discovered in recon WITHOUT rebuilding (TASK-0038).
	Queries PromQueries
	// Labels overrides the Prometheus identity label keys (UUID/Hostname/model/job).
	// Zero value uses the common DCGM-exporter labeling (PromLabels.withDefaults).
	// Only consulted when PrometheusURL is set. Lets an operator align to a
	// non-default dcgm-exporter relabeling without rebuilding (TASK-0038).
	Labels PromLabels

	// Spec is the operator's STATIC device-spec (TASK-0038): per-GPU-model or
	// per-UUID {peak_tflops, cost_usd_per_hour}. When the real Prom/DCGM path
	// supplies no peak/cost, the spec fills PeakFLOPS/CostPerHour so MFU + $/hr
	// render real, stamped with provenance (peak.source/cost.source=static-spec).
	// A real series ALWAYS wins over the spec; an empty spec is a transparent
	// passthrough (no-spec behavior unchanged). Applied to BOTH the real chain and
	// the mock default, so a spec completes whichever metrics source is active.
	Spec DeviceSpec

	// PeakTable, when true, enables the BUILT-IN GPU peak-FLOPS table (TASK-0044):
	// the LAST resort before degrade. After the real chain AND the operator spec
	// have had first refusal, a device whose peak is STILL unknown has it resolved
	// from the datasheet table keyed by DCGM modelName (FP16/BF16 dense), stamped
	// peak.source=builtin-table:<model>@fp16-dense. An unknown model DEGRADES (no
	// guess). It is FLOPs-only — $/hr stays operator-supplied. Default true so a
	// zero-config box gets real MFU; an operator may disable it for pure
	// degrade-not-fabricate. It wraps AFTER SpecFillCollector so the strict
	// precedence (real > operator-explicit > built-in table > degrade) holds.
	PeakTable bool

	// NCCLLogPath, when set, points the log/event collector at an NCCL log file
	// (read-only tail). Empty ⇒ NCCL stream unavailable (degrade). dmesg/kmsg is
	// only read on the gpu build; on the default build the log source has no kmsg.
	NCCLLogPath string

	// ProfilingBurst opts the DCGM fallback into high-resolution profiling
	// scrapes, frequency-capped by ProfilingCap (RULES §A / D-0009 profiling cap).
	// Default false: the daemon's own interval is the rate limiter.
	ProfilingBurst bool
	// ProfilingCap is the minimum interval between profiling-burst scrapes. Zero
	// uses DefaultProfilingBurstInterval. The cap guarantees the agent can never
	// drive the exporter faster than this — the off-critical-path linchpin.
	ProfilingCap time.Duration
}

// DefaultPromQueries returns the standard PromQL instant-query expressions for a
// DCGM-exporter-labeled Prometheus. They are pure reads over already-scraped
// series (zero new scrape load). Each can be overridden per RuntimeConfig.Queries;
// an expression left empty is skipped and the field degrades.
func DefaultPromQueries() PromQueries {
	return PromQueries{
		TensorActive:  "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE",
		AchievedFLOPs: "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE * DCGM_FI_DEV_TENSOR_PEAK_FLOPS",
		PeakFLOPS:     "DCGM_FI_DEV_TENSOR_PEAK_FLOPS",
		CostPerHour:   "gpufleet_device_cost_usd_per_hour",
		JobOwner:      "gpufleet_device_job",
	}
}

// hasRealEndpoint reports whether any real telemetry endpoint is configured.
func (c RuntimeConfig) hasRealEndpoint() bool {
	return c.PrometheusURL != "" || c.DCGMExporterURL != ""
}

// useReal resolves the effective collector mode to a boolean: true ⇒ real HTTP
// collectors, false ⇒ mock DefaultCollectors. Auto uses real iff an endpoint is
// configured; mock/real force the choice.
func (c RuntimeConfig) useReal() bool {
	switch c.Mode {
	case CollectorModeMock:
		return false
	case CollectorModeReal:
		return true
	default: // CollectorModeAuto or unset
		return c.hasRealEndpoint()
	}
}

// Collectors builds the daemon's collector list from the runtime config. It
// returns the REAL HTTP MetricsChain (Prometheus-first → DCGM fallback) + the
// log/event collector when real collection is selected, else the MOCK
// DefaultCollectors. It never dials an endpoint here (read-only construction
// only); an unreachable endpoint surfaces at Collect and degrades through the
// existing chain. The chosen mode is reported for provenance/logging.
func (c RuntimeConfig) Collectors() (cols []Collector, mode CollectorMode) {
	if !c.useReal() {
		mock := DefaultCollectors(c.Node)
		// An operator may pair a static spec with the mock default (e.g. a homogeneous
		// box demo): the spec then completes any peak/cost the mock left unknown,
		// stamped static-spec. With NO spec this is a transparent passthrough, so the
		// no-endpoint mock default stays byte-for-byte backward compatible (demo1).
		// The built-in peak table (TASK-0044) is DELIBERATELY NOT applied on the mock
		// path: the mock's synthetic "A10" devices include an intentionally
		// peak-degraded device (GPU-mock-0003) that the demo relies on to show the
		// degrade-mark; auto-resolving it from the table would alter the demo's
		// mock-default behavior. The table is a REAL-telemetry convenience only.
		return c.applySpec(mock), CollectorModeMock
	}

	queries := c.Queries
	if queries == (PromQueries{}) {
		queries = DefaultPromQueries()
	}

	// Primary: existing Prometheus (preferred, zero new scrape load). nil when no
	// URL given, so the chain has only the DCGM fallback. Labels align to the real
	// dcgm-exporter schema discovered in recon (zero value ⇒ common defaults).
	var primary Collector
	if c.PrometheusURL != "" {
		primary = PrometheusCollector{
			BaseURL: c.PrometheusURL,
			Node:    c.Node,
			Queries: queries,
			Labels:  c.Labels,
		}
	}

	// Fallback: local DCGM-exporter scrape (NVML-free). nil when no URL given.
	var fallback Collector
	if c.DCGMExporterURL != "" {
		fallback = &DCGMExporterCollector{
			ScrapeURL:      c.DCGMExporterURL,
			Node:           c.Node,
			ProfilingBurst: c.ProfilingBurst,
			BurstCap:       c.ProfilingCap,
		}
	}

	// The log/event collector rounds out the >=2-signal picture (dmesg/XID +
	// NCCL). realLogSource is a build-tagged seam (TASK-0056): on the gpu build it
	// tails the REAL /dev/kmsg (+ optional NCCL file) so `--collectors real`
	// genuinely collects the dmesg/XID leg (before this, the real branch hardcoded
	// a fixture source → read no kmsg → XID79/ECC-XID could never fire on real HW);
	// on the default build it degrades to a fixture/NCCL-file source (no kmsg).
	// READ-ONLY per-window; a missing/garbage stream degrades, never crashes.
	logSrc := realLogSource(c.NCCLLogPath)

	// Wrap the metrics chain with the static-spec fill (TASK-0038): when the real
	// chain leaves peak/cost unknown, the operator's spec completes it (real wins;
	// empty spec ⇒ passthrough), stamped static-spec for auditability. Then wrap
	// the built-in peak table (TASK-0044) OUTSIDE the spec, so the strict
	// precedence holds: real series (inner) > operator spec (SpecFill) > built-in
	// table (PeakTable, last resort) > degrade. The table is FLOPs-only.
	var metrics Collector = NewMetricsChain(primary, fallback)
	if !c.Spec.Empty() {
		metrics = SpecFillCollector{Inner: metrics, Spec: c.Spec}
	}
	if c.PeakTable {
		metrics = PeakTableCollector{Inner: metrics, Enabled: true}
	}
	cols = []Collector{
		metrics,
		LogEventCollector{Src: logSrc, Node: c.Node},
		// The PROC/sysfs link-health collector (TASK-0053) is the INDEPENDENT,
		// non-DCGM `link.degraded.*` leg of the LINK_DEGRADED gate. It is a real
		// read-only file reader (sysfs PCIe link width/speed) — no network endpoint,
		// no GPU — so it belongs in the real set unconditionally; a box without sysfs
		// (or with no degraded link) simply emits no leg and the gate ABSTAINs.
		ProcLinkCollector{Node: c.Node},
	}
	return cols, CollectorModeReal
}

// applySpec wraps each metrics collector in `cols` with SpecFillCollector when a
// non-empty static spec is configured, leaving non-metrics collectors (log/event)
// untouched. With an empty spec it returns `cols` unchanged — a transparent
// passthrough preserving exact backward compatibility (the mock default path).
// A metrics collector is identified by its SignalSource (Prometheus or DCGM).
// The built-in peak table is intentionally NOT applied here (mock path) — see
// Collectors(); only the operator's explicit spec fills the mock default. With an
// empty spec, `cols` is returned unchanged (byte-for-byte backward compatible).
func (c RuntimeConfig) applySpec(cols []Collector) []Collector {
	if c.Spec.Empty() {
		return cols
	}
	out := make([]Collector, len(cols))
	for i, col := range cols {
		switch col.Source() {
		case gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS,
			gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM:
			out[i] = SpecFillCollector{Inner: col, Spec: c.Spec}
		default:
			out[i] = col
		}
	}
	return out
}

// fileNCCLSource is a read-only LogSource that tails an NCCL log FILE on the
// default build. It exposes no kmsg (the kernel ring buffer is a gpu-build
// concern). Each read opens the file O_RDONLY, reads up to ncclMaxBytes, and
// closes it; a missing/unreadable file yields a nil reader so the parser
// degrades rather than failing. Strictly read-only — never writes back.
type fileNCCLSource struct{ path string }

// ncclMaxBytes bounds a single NCCL log read so a huge/rotated log can never
// stall or balloon the collector (read-only off-path bound, RULES §A).
const ncclMaxBytes = 4 << 20 // 4 MiB

func (s fileNCCLSource) Kmsg() (io.Reader, error) { return nil, nil }

func (s fileNCCLSource) NCCL() (io.Reader, error) {
	if s.path == "" {
		return nil, nil
	}
	f, err := os.Open(s.path) // O_RDONLY: read-only tail, never writes
	if err != nil {
		return nil, nil // unavailable ⇒ degrade, never fatal
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, ncclMaxBytes))
	if err != nil || len(b) == 0 {
		return nil, nil
	}
	return bytes.NewReader(b), nil
}
