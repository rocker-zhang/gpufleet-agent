// Command agent is the read-only gpufleet collector/sidecar (demo1 data-plane
// spine). It runs the mock signal sources by default (NVML-backed only with
// -tags gpu), normalizes them into a gpufleet.v1 SignalSchema window, and:
//
//   - with -serve: runs the headless refresh daemon and exposes the LOCAL
//     READ-ONLY HTTP API (latest signal window + standalone cost wedge) for the
//     cli bypass viewer.
//   - otherwise: does one collection cycle and prints the window + cost as JSON.
//
// It is off-critical-path and never controls, schedules, throttles, or kills a
// GPU/job. The local API is read-only (GET only); it never originates egress to
// the control plane UNLESS -investigate-url is explicitly set (OFF by default).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	agent "github.com/rocker-zhang/gpufleet-agent"
	"github.com/rocker-zhang/gpufleet-agent/internal/investigate"
	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	node := flag.String("node", envStr("GPUFLEET_NODE", ""), "node name to stamp on emitted evidence")
	serve := flag.Bool("serve", false, "run the headless daemon + local read-only HTTP API")
	addr := flag.String("addr", "127.0.0.1:0", "localhost address for the read-only API (with -serve)")
	window := flag.Duration("window", 60*time.Second, "collection window duration")
	interval := flag.Duration("interval", 15*time.Second, "headless refresh interval (with -serve)")
	// TASK-0040 — data-freshness/staleness. Age (since the last SUCCESSFUL
	// collection) beyond which /cost + /healthz report stale=true while still
	// serving the last-known window (never blanked/fabricated). 0 ⇒ derive a safe
	// default of max(3×interval, 5s) so it tracks the refresh cadence.
	stalenessAfter := flag.Duration("staleness-after", envDur("GPUFLEET_STALENESS_AFTER", 0),
		"age since last SUCCESSFUL collection beyond which data is marked stale; 0 = max(3×interval, 5s); env GPUFLEET_STALENESS_AFTER")

	// TASK-0037 — runtime endpoint wiring. Point the agent at REAL telemetry.
	// HTTP scrape only (no NVML): the real Prometheus/DCGM collectors compile in
	// the DEFAULT !gpu build, so these work without -tags gpu. Each flag also
	// reads an equivalent env var so a DaemonSet can configure via env.
	promURL := flag.String("prometheus-url", envStr("GPUFLEET_PROMETHEUS_URL", ""),
		"EXISTING Prometheus server root for read-only PromQL collection (e.g. http://prometheus:9090); env GPUFLEET_PROMETHEUS_URL")
	dcgmURL := flag.String("dcgm-exporter-url", envStr("GPUFLEET_DCGM_EXPORTER_URL", ""),
		"local DCGM-exporter /metrics endpoint for read-only fallback scrape (e.g. http://localhost:9400/metrics); env GPUFLEET_DCGM_EXPORTER_URL")
	collectors := flag.String("collectors", envStr("GPUFLEET_COLLECTORS", "auto"),
		"collector set: auto|mock|real (auto = real if any endpoint given, else mock); env GPUFLEET_COLLECTORS")
	ncclLog := flag.String("nccl-log", envStr("GPUFLEET_NCCL_LOG", ""),
		"NCCL log file to tail read-only (real collectors only); env GPUFLEET_NCCL_LOG")
	// TASK-0048 — M3 demo vehicle. Swap the mock sources for fault-injecting mock
	// sources that emit a FULL 2-independent-source pattern (ECC + XID79) so the
	// local rca gate FIRES a real Verdict on /verdict. Mock build only (no effect
	// on the real/gpu path); the default ABSTAINs (clean node).
	injectFaults := flag.Bool("inject-faults", envBool("GPUFLEET_INJECT_FAULTS", false),
		"DEMO/LAB: inject a 2-source ECC+XID79 fault pattern (mock build only) so the gate FIRES; env GPUFLEET_INJECT_FAULTS")
	profilingBurst := flag.Bool("profiling-burst", envBool("GPUFLEET_PROFILING_BURST", false),
		"opt into high-resolution DCGM profiling scrapes, frequency-capped by --profiling-cap; env GPUFLEET_PROFILING_BURST")
	profilingCap := flag.Duration("profiling-cap", envDur("GPUFLEET_PROFILING_CAP", agent.DefaultProfilingBurstInterval),
		"minimum interval between profiling-burst scrapes (off-critical-path cap); env GPUFLEET_PROFILING_CAP")

	// TASK-0038 — static device-spec ($/hr is an operator PRICE input; peak-FLOPS
	// for a known GPU model is a static spec). When the real Prom/DCGM path gives
	// no peak/cost, fill from the spec so MFU + $/hr render real, stamped
	// static-spec for auditability. A real series always wins over the spec.
	deviceSpecFile := flag.String("device-spec-file", envStr("GPUFLEET_DEVICE_SPEC_FILE", ""),
		"static device-spec JSON: per-GPU-model or per-UUID {peak_tflops, cost_usd_per_hour} filling missing peak/cost; env GPUFLEET_DEVICE_SPEC_FILE")
	costPerHour := flag.Float64("cost-usd-per-hour", envFloat("GPUFLEET_COST_USD_PER_HOUR", 0),
		"homogeneous-box $/hour rate applied to every device missing a cost (quick alternative to a spec file); env GPUFLEET_COST_USD_PER_HOUR")
	peakTFLOPS := flag.Float64("peak-tflops", envFloat("GPUFLEET_PEAK_TFLOPS", 0),
		"homogeneous-box peak TFLOP/s applied to every device missing a peak (quick alternative to a spec file); env GPUFLEET_PEAK_TFLOPS")

	// TASK-0044 — built-in GPU peak-FLOPS table. The LAST resort before degrade:
	// after the real chain and any operator spec/--peak-tflops, a device whose peak
	// is STILL unknown has it auto-resolved from the datasheet table by DCGM
	// modelName (FP16/BF16 dense), stamped builtin-table:<model>@fp16-dense. So a
	// zero-config A10 box gets real MFU with NO --peak-tflops. Default ON; disable
	// for pure degrade-not-fabricate. FLOPs-only: $/hr stays operator-supplied.
	builtinPeakTable := flag.Bool("builtin-peak-table", envBool("GPUFLEET_BUILTIN_PEAK_TABLE", true),
		"auto-resolve a missing peak from the built-in datasheet table by GPU modelName (FP16/BF16 dense); operator --peak-tflops/--device-spec-file and real telemetry still win; env GPUFLEET_BUILTIN_PEAK_TABLE")

	// TASK-0061 — multi-round directive escalation loop (D-0011 / M5). When
	// -investigate-url is set, the agent POSTs its EvidencePack to the control
	// plane after every refresh cycle and drives the ABSTAIN→FIRE convergence
	// loop. OFF by default — the daemon stays read-only unless this flag is set.
	// The Bearer token is read from env GPUFLEET_INVESTIGATE_TOKEN ONLY (no flag)
	// so it is never visible in the process argument list or flag dumps.
	investigateURL := flag.String("investigate-url", envStr("GPUFLEET_INVESTIGATE_URL", ""),
		"control-plane base URL for the directive escalation loop, e.g. https://api.gpufleet.sg; OFF when empty; env GPUFLEET_INVESTIGATE_URL")

	// TASK-0038 — query/label override so an operator aligns to the real
	// dcgm-exporter schema discovered in recon WITHOUT rebuilding.
	qTensorActive := flag.String("query-tensor-active", envStr("GPUFLEET_QUERY_TENSOR_ACTIVE", ""),
		"override PromQL for tensor-active; env GPUFLEET_QUERY_TENSOR_ACTIVE")
	qAchievedFLOPs := flag.String("query-achieved-flops", envStr("GPUFLEET_QUERY_ACHIEVED_FLOPS", ""),
		"override PromQL for achieved FLOP/s; env GPUFLEET_QUERY_ACHIEVED_FLOPS")
	qPeakFLOPS := flag.String("query-peak-flops", envStr("GPUFLEET_QUERY_PEAK_FLOPS", ""),
		"override PromQL for peak FLOP/s; env GPUFLEET_QUERY_PEAK_FLOPS")
	qCostPerHour := flag.String("query-cost-per-hour", envStr("GPUFLEET_QUERY_COST_PER_HOUR", ""),
		"override PromQL for $/hour rate; env GPUFLEET_QUERY_COST_PER_HOUR")
	qJobOwner := flag.String("query-job-owner", envStr("GPUFLEET_QUERY_JOB_OWNER", ""),
		"override PromQL for device->job ownership; env GPUFLEET_QUERY_JOB_OWNER")
	labelUUID := flag.String("label-uuid", envStr("GPUFLEET_LABEL_UUID", ""),
		"override the Prometheus device-UUID label key (default UUID); env GPUFLEET_LABEL_UUID")
	labelNode := flag.String("label-node", envStr("GPUFLEET_LABEL_NODE", ""),
		"override the Prometheus hostname label key (default Hostname); env GPUFLEET_LABEL_NODE")
	labelModel := flag.String("label-model", envStr("GPUFLEET_LABEL_MODEL", ""),
		"override the Prometheus model label key (default modelName); env GPUFLEET_LABEL_MODEL")
	labelJob := flag.String("label-job", envStr("GPUFLEET_LABEL_JOB", ""),
		"override the Prometheus job label key (default job); env GPUFLEET_LABEL_JOB")
	flag.Parse()

	spec, err := buildDeviceSpec(*deviceSpecFile, *peakTFLOPS, *costPerHour)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent: device-spec: %v\n", err)
		os.Exit(1)
	}

	rc := agent.RuntimeConfig{
		Node:            *node,
		PrometheusURL:   *promURL,
		DCGMExporterURL: *dcgmURL,
		Mode:            agent.CollectorMode(*collectors),
		NCCLLogPath:     *ncclLog,
		ProfilingBurst:  *profilingBurst,
		ProfilingCap:    *profilingCap,
		Queries: overrideQueries(agent.PromQueries{
			TensorActive:  *qTensorActive,
			AchievedFLOPs: *qAchievedFLOPs,
			PeakFLOPS:     *qPeakFLOPS,
			CostPerHour:   *qCostPerHour,
			JobOwner:      *qJobOwner,
		}),
		Labels: agent.PromLabels{
			UUID:  *labelUUID,
			Node:  *labelNode,
			Model: *labelModel,
			Job:   *labelJob,
		},
		Spec:      spec,
		PeakTable: *builtinPeakTable,
	}
	cols, mode := rc.Collectors()
	if *injectFaults {
		if fc := faultInjectCollectors(*node); fc != nil {
			cols, mode = fc, "mock+inject-faults"
		} else {
			fmt.Fprintf(os.Stderr, "agent: --inject-faults ignored (mock build only)\n")
		}
	}
	fmt.Fprintf(os.Stderr, "agent: collectors=%s (prometheus-url=%q dcgm-exporter-url=%q)\n",
		mode, *promURL, *dcgmURL)

	// Resolve the staleness threshold (TASK-0040). When the flag/env is unset (0),
	// derive max(3×interval, 5s) from the refresh cadence — three missed
	// collections, floored so a fast interval does not flap to stale on jitter.
	staleAfter := *stalenessAfter
	if staleAfter <= 0 {
		staleAfter = 3 * *interval
		if floor := 5 * time.Second; staleAfter < floor {
			staleAfter = floor
		}
	}

	d := agent.NewDaemon(agent.DaemonConfig{
		AgentID:        "gpufleet-agent",
		Window:         *window,
		StalenessAfter: staleAfter,
		Collectors:     cols,
		Policy:         agent.DefaultCostPolicy(),
	})

	if !*serve {
		// One-shot: collect, normalize, cost, print.
		if err := d.Refresh(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "agent: refresh failed: %v\n", err)
			os.Exit(1)
		}
		st := d.Snapshot()
		out := map[string]any{
			"signals_json": json.RawMessage(mustProtoJSON(st)),
			"cost":         st.Cost,
			"degraded":     st.Window.Degraded,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fmt.Fprintf(os.Stderr, "agent: encode failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Headless refresh loop runs regardless of whether any client connects.
	go func() { _ = d.Run(ctx, *interval) }()

	// TASK-0061 — opt-in egress to the control-plane investigate loop (OFF by
	// default; only runs when -investigate-url is set). The daemon stays
	// read-only unless this flag is explicitly provided. The Bearer token is
	// read from the environment ONLY — never from a flag to keep it out of
	// ps/arg-list exposure. It is never written to any log.
	if *investigateURL != "" {
		// SECURITY: token comes from env only. Never log or print this value.
		token := os.Getenv("GPUFLEET_INVESTIGATE_TOKEN")
		ic := &investigate.Client{
			URL:    *investigateURL,
			Token:  token,
			NodeID: *node,
			// Tier-0 only by default; Tier-1 (ebpf.nvlink.retrans) requires a
			// separate operator opt-in not yet wired (future step).
			Consent:  tier0Consent,
			Executor: investigate.NoopExecutor{},
		}
		fmt.Fprintf(os.Stderr, "agent: investigate-url=%q (egress enabled, token=<redacted>)\n", *investigateURL)
		go runInvestigateLoop(ctx, d, ic, *interval)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           agent.NewAPI(d).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	fmt.Fprintf(os.Stderr, "agent: local read-only API listening on %s (GET /signals /cost /verdict /window /healthz); staleness-after=%s\n", *addr, staleAfter)
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "agent: serve failed: %v\n", err)
		os.Exit(1)
	}
}

// envStr returns the env var `key` if set and non-empty, else `def`. The flag
// default is derived from env so a DaemonSet can configure via env OR flag (the
// flag, when passed, wins). Read-only config plumbing.
func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envBool parses a boolean env var (1/true/yes/on accepted, case-insensitive),
// falling back to `def` when unset or unparseable.
func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	return def
}

// envDur parses a duration env var (e.g. "500ms", "2s"), falling back to `def`
// when unset or unparseable.
func envDur(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if dur, err := time.ParseDuration(v); err == nil {
		return dur
	}
	return def
}

// envFloat parses a float64 env var, falling back to `def` when unset or
// unparseable. Used for the homogeneous-box --cost-usd-per-hour / --peak-tflops
// quick spec shortcuts.
func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return def
}

// buildDeviceSpec assembles the static device-spec (TASK-0038) from the spec
// file (if any) plus the homogeneous-box quick shortcuts. The file is loaded
// first; the quick --peak-tflops/--cost-usd-per-hour then seed a wildcard
// fallback entry applied to ANY device the file does not name, so a homogeneous
// box needs no per-model JSON. A real series still always wins over either.
func buildDeviceSpec(file string, peakTFLOPS, costPerHour float64) (agent.DeviceSpec, error) {
	spec, err := agent.LoadDeviceSpec(file)
	if err != nil {
		return agent.DeviceSpec{}, err
	}
	if peakTFLOPS > 0 || costPerHour > 0 {
		if spec.ByModel == nil {
			spec.ByModel = map[string]agent.DeviceSpecEntry{}
		}
		// The wildcard model "*" matches any device the spec did not name (a
		// homogeneous-box convenience). lookup() consults named entries first.
		spec.ByModel[agent.WildcardModel] = agent.DeviceSpecEntry{
			PeakTFLOPS:  peakTFLOPS,
			CostPerHour: costPerHour,
		}
	}
	return spec, nil
}

// overrideQueries fills any query field the operator left empty with the
// DefaultPromQueries value, so an operator can override ONE expression (e.g.
// align CostPerHour to a real series name) without blanking the rest. An
// all-empty input ⇒ the full defaults.
func overrideQueries(q agent.PromQueries) agent.PromQueries {
	def := agent.DefaultPromQueries()
	if q.TensorActive == "" {
		q.TensorActive = def.TensorActive
	}
	if q.AchievedFLOPs == "" {
		q.AchievedFLOPs = def.AchievedFLOPs
	}
	if q.PeakFLOPS == "" {
		q.PeakFLOPS = def.PeakFLOPS
	}
	if q.CostPerHour == "" {
		q.CostPerHour = def.CostPerHour
	}
	if q.JobOwner == "" {
		q.JobOwner = def.JobOwner
	}
	return q
}

func mustProtoJSON(st *agent.State) []byte {
	b, err := protojson.Marshal(st.Window.Pack)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent: marshal pack: %v\n", err)
		os.Exit(1)
	}
	return b
}

// tier0Consent is the default consent gate for the investigate client: only
// Tier-0 (UNPRIVILEGED) capabilities are enabled; Tier-1 (PRIVILEGED, e.g.
// ebpf.nvlink.retrans) requires explicit operator opt-in (not yet wired).
func tier0Consent(t gpufleetv1.ConsentTier) bool {
	return t == gpufleetv1.ConsentTier_CONSENT_TIER_UNPRIVILEGED
}

// runInvestigateLoop runs alongside the daemon's refresh loop and, whenever the
// daemon has a new window, POSTs its EvidencePack to the control-plane
// investigate endpoint, driving the multi-round directive escalation loop.
//
// It is off-critical-path (RULES §A): errors are logged but never stop the
// daemon loop or affect a customer job. Each investigate call runs in its own
// goroutine so a slow round-trip cannot block the next refresh cycle. The loop
// itself terminates when ctx is cancelled (graceful shutdown).
func runInvestigateLoop(ctx context.Context, d *agent.Daemon, ic *investigate.Client, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	var lastRefreshes uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st := d.Snapshot()
			if st == nil || st.Window == nil || st.Window.Pack == nil {
				continue
			}
			if st.Refreshes == lastRefreshes {
				continue // same window as last time; nothing new to investigate.
			}
			lastRefreshes = st.Refreshes
			pack := st.Window.Pack
			// Run investigate in its own goroutine: a slow network round-trip must
			// not block the next refresh tick or stall the daemon's local API.
			go func(p *gpufleetv1.EvidencePack) {
				ictx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				verdict, err := ic.Investigate(ictx, p)
				if err != nil {
					fmt.Fprintf(os.Stderr, "agent: investigate: %v\n", err)
					return
				}
				if verdict != nil {
					fmt.Fprintf(os.Stderr, "agent: investigate: verdict=%s\n",
						verdict.GetFaultClass())
				}
			}(pack)
		}
	}
}
