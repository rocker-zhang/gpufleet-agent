// Command agent is the read-only gpufleet collector/sidecar. It reads a
// metrics source (mock by default; NVML-backed only with -tags gpu) and prints
// a normalized evidence pack as JSON. It is off-critical-path and never
// controls a GPU.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/rocker-zhang/gpufleet-agent"
)

func main() {
	node := flag.String("node", "", "node name to stamp on emitted evidence")
	flag.Parse()

	ev, err := agent.Collect(agent.DefaultReader(*node))
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent: collect failed: %v\n", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(ev); err != nil {
		fmt.Fprintf(os.Stderr, "agent: encode failed: %v\n", err)
		os.Exit(1)
	}
}
