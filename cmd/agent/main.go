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
// GPU/job. The local API is read-only (GET only); it never originates egress.
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
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	node := flag.String("node", envStr("GPUFLEET_NODE", ""), "node name to stamp on emitted evidence")
	serve := flag.Bool("serve", false, "run the headless daemon + local read-only HTTP API")
	addr := flag.String("addr", "127.0.0.1:0", "localhost address for the read-only API (with -serve)")
	window := flag.Duration("window", 60*time.Second, "collection window duration")
	interval := flag.Duration("interval", 15*time.Second, "headless refresh interval (with -serve)")

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
	profilingBurst := flag.Bool("profiling-burst", envBool("GPUFLEET_PROFILING_BURST", false),
		"opt into high-resolution DCGM profiling scrapes, frequency-capped by --profiling-cap; env GPUFLEET_PROFILING_BURST")
	profilingCap := flag.Duration("profiling-cap", envDur("GPUFLEET_PROFILING_CAP", agent.DefaultProfilingBurstInterval),
		"minimum interval between profiling-burst scrapes (off-critical-path cap); env GPUFLEET_PROFILING_CAP")
	flag.Parse()

	rc := agent.RuntimeConfig{
		Node:            *node,
		PrometheusURL:   *promURL,
		DCGMExporterURL: *dcgmURL,
		Mode:            agent.CollectorMode(*collectors),
		NCCLLogPath:     *ncclLog,
		ProfilingBurst:  *profilingBurst,
		ProfilingCap:    *profilingCap,
	}
	cols, mode := rc.Collectors()
	fmt.Fprintf(os.Stderr, "agent: collectors=%s (prometheus-url=%q dcgm-exporter-url=%q)\n",
		mode, *promURL, *dcgmURL)

	d := agent.NewDaemon(agent.DaemonConfig{
		AgentID:    "gpufleet-agent",
		Window:     *window,
		Collectors: cols,
		Policy:     agent.DefaultCostPolicy(),
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

	srv := &http.Server{Addr: *addr, Handler: agent.NewAPI(d).Handler()}
	fmt.Fprintf(os.Stderr, "agent: local read-only API listening on %s (GET /signals /cost /window /healthz)\n", *addr)
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

func mustProtoJSON(st *agent.State) []byte {
	b, err := protojson.Marshal(st.Window.Pack)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent: marshal pack: %v\n", err)
		os.Exit(1)
	}
	return b
}
