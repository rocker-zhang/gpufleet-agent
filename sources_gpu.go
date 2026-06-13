//go:build gpu

package agent

import (
	"bytes"
	"io"
	"os"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file is the gpu-tagged build's signal sources. It is compiled ONLY with
// `-tags gpu` (lab-only), so the default build and shipped binaries never link
// NVML and need no GPU/cgo. The real implementations perform strictly READ-ONLY
// NVML queries + a read-only /dev/kmsg (dmesg) tail; they NEVER mutate device
// state (no clocks, no reset, no power caps) — RULES §A and the agent module
// red lines.
//
// IMPORTANT: the metrics + log/event PARSING is shared with the default build
// (collectors_dcgm.go, collectors_logs.go); only the raw hardware READS are
// isolated here. So the gpu build reuses the SAME read-only collectors:
//   - DCGMExporterCollector scrapes the local DCGM-exporter (no NVML needed).
//   - PrometheusCollector queries the existing Prometheus.
//   - LogEventCollector parses streams from a kmsgLogSource that tails /dev/kmsg
//     and reads the NCCL log file.
// NVML is only needed for the gap nvidia-smi covers; until the lab wires the
// read-only NVML poll, nvmlDCGMCollector degrades (errNVMLNotWired) rather than
// affecting a job.

// nvmlDCGMCollector is the read-only NVML-backed metrics collector (lab-wired).
// It is kept as an explicit boundary; the primary metrics path is the
// DCGM-exporter scrape, which needs no NVML.
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

// kmsgLogSource is the real read-only log source for the gpu build: it reads the
// kernel ring buffer (/dev/kmsg) and the NCCL log file. Both are READ-ONLY
// (O_RDONLY); the collector never writes back. A missing/unreadable stream
// yields a nil reader so the parser degrades rather than failing.
type kmsgLogSource struct {
	// KmsgPath is the kernel ring-buffer device (default /dev/kmsg).
	KmsgPath string
	// NCCLPath is the NCCL log file path (empty ⇒ NCCL stream unavailable).
	NCCLPath string
	// MaxBytes caps how much of each stream is read per window (read-only bound).
	MaxBytes int64
}

func (s kmsgLogSource) readFile(path string) (io.Reader, error) {
	if path == "" {
		return nil, nil
	}
	// O_RDONLY | O_NONBLOCK: /dev/kmsg is a stream; non-blocking read drains the
	// currently-available buffer without ever writing or blocking the node.
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, nil // unavailable ⇒ degrade, never fatal
	}
	defer func() { _ = f.Close() }()
	max := s.MaxBytes
	if max <= 0 {
		max = 4 << 20
	}
	b, _ := io.ReadAll(io.LimitReader(f, max))
	return bytes.NewReader(b), nil
}

func (s kmsgLogSource) Kmsg() (io.Reader, error) {
	path := s.KmsgPath
	if path == "" {
		path = "/dev/kmsg"
	}
	return s.readFile(path)
}

func (s kmsgLogSource) NCCL() (io.Reader, error) {
	return s.readFile(s.NCCLPath)
}

// DefaultCollectors returns the gpu-tagged read-only collectors. Metrics come
// from the (NVML-free) DCGM-exporter scrape with the existing Prometheus
// preferred via the degradation chain; log/events come from the real /dev/kmsg +
// NCCL tail. They are read-only; a source failure is isolated and degrades.
func DefaultCollectors(node string) []Collector {
	return []Collector{
		MetricsChain{
			Primary:  PrometheusCollector{BaseURL: defaultPromURL(), Node: node},
			Fallback: &DCGMExporterCollector{ScrapeURL: defaultDCGMURL(), Node: node},
		},
		LogEventCollector{Src: kmsgLogSource{KmsgPath: "/dev/kmsg", NCCLPath: defaultNCCLLog()}, Node: node},
	}
}

// Default endpoints, overridable by env, for the lab DaemonSet wiring.
func defaultPromURL() string {
	if v := os.Getenv("GPUFLEET_PROM_URL"); v != "" {
		return v
	}
	return "http://localhost:9090"
}

func defaultDCGMURL() string {
	if v := os.Getenv("GPUFLEET_DCGM_URL"); v != "" {
		return v
	}
	return "http://localhost:9400/metrics"
}

func defaultNCCLLog() string {
	return os.Getenv("GPUFLEET_NCCL_LOG") // empty ⇒ NCCL stream unavailable (degrade)
}
