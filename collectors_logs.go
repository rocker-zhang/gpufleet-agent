package agent

import (
	"bufio"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the build-AGNOSTIC logs & events collector: it actively reads the
// fault-signal gap the metric exporters miss — dmesg/XID (kernel ring buffer),
// NCCL logs, and nvidia-smi/NVML status. The PARSING is pure (no GPU, no cgo) and
// lives here so it is testable on the default build from fixture text. The real
// hardware READS that feed it (tail /dev/kmsg, NVML poll) are isolated behind
// //go:build gpu (see sources_gpu.go / collectors_logs_gpu.go); on the default
// build the source readers come from fixtures or files. It is strictly read-only
// (RULES §A): it tails/reads and emits, never writes back or controls anything.

// LogSource supplies the raw text streams the LogEventCollector parses. Each
// returns an io.Reader for the bytes observed in the window (or nil if that
// stream is unavailable → the normalizer degrades, never fabricates). On the
// default build these are backed by fixtures or read-only file tails; under
// //go:build gpu they tail /dev/kmsg and poll NVML.
type LogSource interface {
	// Kmsg returns the kernel ring-buffer (dmesg) text for the window, or nil.
	Kmsg() (io.Reader, error)
	// NCCL returns the NCCL log text for the window, or nil.
	NCCL() (io.Reader, error)
}

// LogEventCollector reads dmesg/XID and NCCL signals from a LogSource and emits
// them as raw, source-tagged proto events. It is the read-only Collector for the
// dmesg/XID provenance class (its Source is DMESG_XID; NCCL events ride in the
// same observation because they are co-collected node-locally, and the
// normalizer carries them verbatim).
type LogEventCollector struct {
	// Source supplies the raw streams. Required.
	Src LogSource
	// Node stamps the node onto provenance (per-node DaemonSet).
	Node string
}

func (c LogEventCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID
}

// Collect reads and parses the log streams for the window. A stream that is
// unavailable (nil reader / read error) is skipped and marked in provenance —
// degrade, never fabricate. It never returns a hard error for an empty/absent
// stream, so a missing NCCL log can never crash the daemon or affect a job.
func (c LogEventCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	obs := Observation{
		Source: c.Source(),
		Provenance: map[string]string{
			"source": "log-events",
			"node":   c.Node,
		},
	}
	if c.Src == nil {
		obs.Provenance["kmsg"] = "unavailable"
		obs.Provenance["nccl"] = "unavailable"
		return obs, nil
	}

	if r, err := c.Src.Kmsg(); err == nil && r != nil {
		obs.XidEvents = parseKmsgXID(r, now)
		obs.Provenance["kmsg"] = "read"
		obs.Provenance["xid_events"] = strconv.Itoa(len(obs.XidEvents))
	} else {
		obs.Provenance["kmsg"] = "unavailable"
	}

	if r, err := c.Src.NCCL(); err == nil && r != nil {
		obs.NcclEvents = parseNCCL(r, now)
		obs.Provenance["nccl"] = "read"
		obs.Provenance["nccl_events"] = strconv.Itoa(len(obs.NcclEvents))
	} else {
		obs.Provenance["nccl"] = "unavailable"
	}

	return obs, nil
}

// xidLineRE matches the public NVIDIA Xid kernel message shape. Only the public
// XID number + raw text are extracted; NO closed/proprietary error-code semantics
// (RULES §F). Example:
//
//	NVRM: Xid (PCI:0000:65:00): 79, pid=1234, GPU has fallen off the bus
var xidLineRE = regexp.MustCompile(`Xid\s*\(([^)]*)\)\s*:?\s*(\d+)`)

// pciUUIDRE pulls a GPU UUID if the line carries one (some kernels print it).
var gpuUUIDRE = regexp.MustCompile(`GPU-[0-9a-fA-F-]{8,}`)

// parseKmsgXID parses kernel ring-buffer (dmesg/kmsg) text into XidEvents. Each
// matched Xid line yields one event carrying the public XID number and the raw
// line verbatim. Non-Xid lines are ignored. Determinism: events in line order.
func parseKmsgXID(r io.Reader, now time.Time) []*gpufleetv1.XidEvent {
	var events []*gpufleetv1.XidEvent
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1*1024*1024)
	for sc.Scan() {
		raw := sc.Text()
		m := xidLineRE.FindStringSubmatch(raw)
		if m == nil {
			continue
		}
		xid, err := strconv.ParseUint(m[2], 10, 32)
		if err != nil {
			continue
		}
		ev := &gpufleetv1.XidEvent{
			Ts:         timestamppb.New(now),
			Xid:        uint32(xid),
			RawMessage: strings.TrimSpace(raw),
			Severity:   kmsgSeverity(raw),
		}
		if u := gpuUUIDRE.FindString(raw); u != "" {
			ev.DeviceUuid = u
		}
		events = append(events, ev)
	}
	return events
}

// kmsgSeverity extracts a coarse, opaque severity hint from a kmsg line. Best
// effort only; opaque to the engine.
func kmsgSeverity(raw string) string {
	low := strings.ToLower(raw)
	switch {
	case strings.Contains(low, "fatal"), strings.Contains(low, "fallen off the bus"):
		return "err"
	case strings.Contains(low, "warn"):
		return "warn"
	default:
		return "err" // an Xid is an error-class kernel event by default
	}
}

// ncclTimeoutRE / ncclSlowRE match common NCCL watchdog/log lines. These are
// wire-level observations (NOT a fault verdict — RULES §B).
var (
	ncclWatchdogRE = regexp.MustCompile(`(?i)NCCL.*(timeout|timed out|watchdog)`)
	ncclRankRE     = regexp.MustCompile(`(?i)rank\s+(\d+)`)
	ncclOpRE       = regexp.MustCompile(`(?i)(AllReduce|Broadcast|AllGather|ReduceScatter|Reduce|SendRecv)`)
	ncclCommRE     = regexp.MustCompile(`(?i)comm(?:unicator)?\s*[:=]?\s*([0-9a-fA-Fx]+)`)
	ncclDurUsRE    = regexp.MustCompile(`(\d+)\s*(us|ms)`)
)

// parseNCCL parses NCCL log text into NcclEvents. A watchdog/timeout line yields
// an OP_TIMEOUT event; other recognizable collective lines yield OP_COMPLETED
// (wire-level observation only). Unrecognized lines are ignored. Deterministic:
// events in line order.
func parseNCCL(r io.Reader, now time.Time) []*gpufleetv1.NcclEvent {
	var events []*gpufleetv1.NcclEvent
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1*1024*1024)
	for sc.Scan() {
		raw := sc.Text()
		isWatchdog := ncclWatchdogRE.MatchString(raw)
		op := ""
		if m := ncclOpRE.FindStringSubmatch(raw); m != nil {
			op = m[1]
		}
		if !isWatchdog && op == "" {
			continue // not a recognizable NCCL collective line
		}
		ev := &gpufleetv1.NcclEvent{
			Ts:         timestamppb.New(now),
			Op:         op,
			RawMessage: strings.TrimSpace(raw),
			Kind:       gpufleetv1.NcclEventKind_NCCL_EVENT_KIND_OP_COMPLETED,
		}
		if isWatchdog {
			ev.Kind = gpufleetv1.NcclEventKind_NCCL_EVENT_KIND_OP_TIMEOUT
		}
		if m := ncclRankRE.FindStringSubmatch(raw); m != nil {
			if n, err := strconv.ParseUint(m[1], 10, 32); err == nil {
				ev.Rank = uint32(n)
			}
		}
		if m := ncclCommRE.FindStringSubmatch(raw); m != nil {
			ev.CommunicatorId = m[1]
		}
		if m := ncclDurUsRE.FindStringSubmatch(raw); m != nil {
			if n, err := strconv.ParseUint(m[1], 10, 64); err == nil {
				if strings.EqualFold(m[2], "ms") {
					n *= 1000
				}
				ev.DurationUs = n
			}
		}
		events = append(events, ev)
	}
	return events
}

// FixtureLogSource is a LogSource backed by in-memory fixture text. It is the
// default-build/test source: it feeds canned dmesg/NCCL payloads through the
// same parsers the gpu-tagged build feeds from /dev/kmsg and the NCCL log file.
// Empty fields yield a nil reader (the stream is treated as unavailable →
// degrade).
type FixtureLogSource struct {
	KmsgText string
	NCCLText string
}

func (f FixtureLogSource) Kmsg() (io.Reader, error) {
	if f.KmsgText == "" {
		return nil, nil
	}
	return strings.NewReader(f.KmsgText), nil
}

func (f FixtureLogSource) NCCL() (io.Reader, error) {
	if f.NCCLText == "" {
		return nil, nil
	}
	return strings.NewReader(f.NCCLText), nil
}
