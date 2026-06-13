//go:build gpu

package agent

import (
	"io"
	"os"
	"syscall"
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
	// Zero ⇒ kmsgDefaultMaxBytes.
	MaxBytes int64
	// Deadline bounds wall-clock time spent draining each stream. Zero ⇒
	// kmsgDefaultDeadline. This is the no-stall ceiling for the never-EOF
	// /dev/kmsg stream (RULES §A; module CLAUDE.md kill-switch rule).
	Deadline time.Duration
	// kill is the shared kill-switch holder. The daemon wires its shutdown channel
	// (ctx.Done()) into this via SetKill, so a shutdown/operator abort interrupts
	// an in-progress drain at the next chunk boundary and degrades on the partial
	// bytes. Held BY POINTER so a value-copy of this source (it is stored inside a
	// LogEventCollector / []Collector by value) still observes the wired channel.
	// Nil/empty ⇒ no kill-switch (the deadline + byte cap still bound the read).
	kill *killCell
}

// SetKill wires the daemon's kill-switch channel into this source so a shutdown /
// operator abort can interrupt an in-flight /dev/kmsg drain (TASK-0033 #1). It is
// the killWirable implementation the daemon discovers via LogEventCollector.
//
// The shared cell is allocated at construction (newKmsgLogSource / the
// DefaultCollectors fixture below); SetKill therefore only mutates the cell via
// its lock-guarded set, never the s.kill field itself. This keeps SetKill
// race-safe regardless of call-site ordering (TASK-0034 #2) — the previous lazy
// `s.kill = &killCell{}` was an unsynchronized field write. killCell.set is
// nil-safe, so a source built without a cell still degrades (no kill-switch)
// rather than racing.
func (s *kmsgLogSource) SetKill(kill <-chan struct{}) {
	s.kill.set(kill)
}

// newKmsgLogSource constructs a kmsgLogSource with its shared killCell already
// allocated, so the daemon's wireKill -> SetKill (which may run concurrently with
// a drain's killCell.get) never has to allocate s.kill under no lock. Allocating
// at construction is the race-safe alternative to a guarded lazy-alloc.
func newKmsgLogSource(kmsgPath, ncclPath string) *kmsgLogSource {
	return &kmsgLogSource{
		KmsgPath: kmsgPath,
		NCCLPath: ncclPath,
		kill:     &killCell{},
	}
}

// readFile opens path read-only and NON-BLOCKING and drains it through the
// build-agnostic boundedReadStream so the collector goroutine can NEVER stall the
// node, even on /dev/kmsg (a never-EOF stream). The bounded-read LOGIC is shared
// with the default build and unit-tested there (collectors_kmsg_test.go); here we
// only supply the real O_RDONLY|O_NONBLOCK handle.
func (s kmsgLogSource) readFile(path string) (io.Reader, error) {
	if path == "" {
		return nil, nil
	}
	// O_RDONLY | O_NONBLOCK: /dev/kmsg is a never-EOF stream; the non-blocking
	// open is what lets each Read return promptly with whatever is buffered
	// instead of blocking the OS thread. Read-only: the collector never writes.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil // unavailable ⇒ degrade, never fatal
	}
	defer func() { _ = f.Close() }()
	r, _ := boundedReadToReader(f, boundedReadConfig{
		MaxBytes: s.MaxBytes,
		Deadline: s.Deadline,
		Kill:     s.kill.get(), // nil-safe: no cell / unwired ⇒ nil ⇒ deadline+cap bound
	})
	return r, nil
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
		NewMetricsChain(
			PrometheusCollector{BaseURL: defaultPromURL(), Node: node},
			&DCGMExporterCollector{ScrapeURL: defaultDCGMURL(), Node: node},
		),
		LogEventCollector{Src: newKmsgLogSource("/dev/kmsg", defaultNCCLLog()), Node: node},
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
