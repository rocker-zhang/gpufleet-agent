//go:build !gpu

package main

import "github.com/rocker-zhang/gpufleet-agent"

// faultInjectCollectors returns the fault-injecting mock collectors (M3 demo:
// a 2-independent-source ECC + XID79 pattern that makes the local gate FIRE).
// Available only on the default mock (no-GPU) build.
func faultInjectCollectors(node string) []agent.Collector {
	return agent.FaultInjectCollectors(node)
}
