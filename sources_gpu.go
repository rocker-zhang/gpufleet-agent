//go:build gpu

package agent

import (
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file is the gpu-tagged build's signal sources. It is compiled ONLY with
// `-tags gpu` (lab-only), so the default build and shipped binaries never link
// NVML and need no GPU/cgo. The real implementations perform strictly READ-ONLY
// NVML queries + a read-only /dev/kmsg (dmesg) tail; they NEVER mutate device
// state (no clocks, no reset, no power caps) — RULES §A and the agent module
// red lines. This stub keeps the tagged build compiling and the boundary
// explicit; the lab wires the real read-only readers (TASK-0023 real sources).

// nvmlDCGMCollector is the read-only NVML/DCGM-backed metrics collector.
type nvmlDCGMCollector struct{ Node string }

func (c nvmlDCGMCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM
}

func (c nvmlDCGMCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	// Real impl: read-only NVML field queries; never mutates the device.
	return Observation{
		Source:     c.Source(),
		Provenance: map[string]string{"source": "nvml-dcgm", "note": "read-only NVML, lab-wired"},
	}, errNVMLNotWired
}

// kmsgXIDCollector is the read-only dmesg/XID collector (tails /dev/kmsg).
type kmsgXIDCollector struct{ Node string }

func (c kmsgXIDCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID
}

func (c kmsgXIDCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	return Observation{
		Source:     c.Source(),
		Provenance: map[string]string{"source": "kmsg-xid", "note": "read-only /dev/kmsg tail, lab-wired"},
	}, errNVMLNotWired
}

// DefaultCollectors returns the gpu-tagged read-only NVML/DCGM + dmesg/XID
// collectors. They are lab-only and read-only; until wired they return
// errNVMLNotWired so the off-path daemon degrades rather than affecting a job.
func DefaultCollectors(node string) []Collector {
	return []Collector{
		nvmlDCGMCollector{Node: node},
		kmsgXIDCollector{Node: node},
	}
}
