package agent

import (
	"bytes"
	"io"
	"os"
	"time"
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
	// when PrometheusURL is set.
	Queries PromQueries

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
		return DefaultCollectors(c.Node), CollectorModeMock
	}

	queries := c.Queries
	if queries == (PromQueries{}) {
		queries = DefaultPromQueries()
	}

	// Primary: existing Prometheus (preferred, zero new scrape load). nil when no
	// URL given, so the chain has only the DCGM fallback.
	var primary Collector
	if c.PrometheusURL != "" {
		primary = PrometheusCollector{
			BaseURL: c.PrometheusURL,
			Node:    c.Node,
			Queries: queries,
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
	// NCCL). On the default build kmsg is unavailable; on the gpu build the caller
	// uses DefaultCollectors' real kmsg source instead. NCCL, when a path is given,
	// is tailed READ-ONLY per-window (a missing/garbage file degrades, never
	// crashes).
	var logSrc LogSource = FixtureLogSource{}
	if c.NCCLLogPath != "" {
		logSrc = fileNCCLSource{path: c.NCCLLogPath}
	}

	cols = []Collector{
		NewMetricsChain(primary, fallback),
		LogEventCollector{Src: logSrc, Node: c.Node},
	}
	return cols, CollectorModeReal
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
