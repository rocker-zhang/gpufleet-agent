//go:build gpu

package main

import "github.com/rocker-zhang/gpufleet-agent"

// faultInjectCollectors is a no-op on the gpu build: fault injection is a mock
// demo vehicle, not part of the real NVML-backed read-only path. Returning nil
// makes --inject-faults a no-op (the caller logs that it was ignored).
func faultInjectCollectors(string) []agent.Collector { return nil }
