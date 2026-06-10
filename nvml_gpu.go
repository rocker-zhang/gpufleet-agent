//go:build gpu

package agent

import "errors"

// nvmlReader is the real NVML-backed metrics source. It is compiled ONLY with
// `-tags gpu`, so the default build and the shipped binaries never link NVML
// and require no GPU. The real implementation (CGO/NVML bindings) is wired in
// the dev lab; this stub keeps the tagged build compiling and the boundary
// explicit: NVML usage here is strictly READ-ONLY query (nvmlDeviceGet*), never
// control (no set clocks, no reset, no power caps).
type nvmlReader struct {
	NodeName string
}

// DefaultReader returns the NVML reader when built with the `gpu` tag.
func DefaultReader(node string) Reader { return nvmlReader{NodeName: node} }

var errNVMLNotWired = errors.New("agent: NVML reader not wired in this build (lab-only)")

func (n nvmlReader) Read() (Evidence, error) {
	// Real impl performs read-only NVML queries and dmesg Xid scraping.
	// It must never mutate device state.
	return Evidence{}, errNVMLNotWired
}
