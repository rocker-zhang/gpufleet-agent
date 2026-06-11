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
	"syscall"
	"time"

	agent "github.com/rocker-zhang/gpufleet-agent"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	node := flag.String("node", "", "node name to stamp on emitted evidence")
	serve := flag.Bool("serve", false, "run the headless daemon + local read-only HTTP API")
	addr := flag.String("addr", "127.0.0.1:0", "localhost address for the read-only API (with -serve)")
	window := flag.Duration("window", 60*time.Second, "collection window duration")
	interval := flag.Duration("interval", 15*time.Second, "headless refresh interval (with -serve)")
	flag.Parse()

	d := agent.NewDaemon(agent.DaemonConfig{
		AgentID:    "gpufleet-agent",
		Window:     *window,
		Collectors: agent.DefaultCollectors(*node),
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

func mustProtoJSON(st *agent.State) []byte {
	b, err := protojson.Marshal(st.Window.Pack)
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent: marshal pack: %v\n", err)
		os.Exit(1)
	}
	return b
}
