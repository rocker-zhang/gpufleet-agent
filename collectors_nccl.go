package agent

import (
	"bytes"
	"io"
	"os"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file is the build-AGNOSTIC, read-only NCCL-log collector (TASK-0053). It
// tails an NCCL log FILE and parses the NCCL watchdog/timeout lines into raw
// NcclEvents, source-tagged SIGNAL_SOURCE_NCCL. It exists so the `nccl.timeout`
// gate leg is attributed to the GENUINE NCCL source (G1 source-attribution).
//
// WHY A DEDICATED COLLECTOR (provenance integrity, RULES §B): historically NCCL
// events rode in on the dmesg/XID LogEventCollector (whose Source is DMESG_XID).
// The G1 attribution in normalize.go only feeds the `nccl.timeout`@NCCL leg from
// observations whose Source is genuinely NCCL, so NCCL events carried on a
// DMESG_XID observation are CORRECTLY gated out (a single dmesg collector must not
// mint an NCCL-sourced leg). This collector is the real NCCL feed: its Source() is
// SIGNAL_SOURCE_NCCL, so the watchdog/timeout it observes legitimately mints the
// `nccl.timeout.<scope>`@NCCL leg.
//
// HONESTY (RULES §B): it reads a real NCCL log file and emits ONLY the events the
// log genuinely contains. A missing/unreadable/empty log yields no events
// (degrade, never fabricate). It is strictly read-only (O_RDONLY tail, bounded by
// ncclMaxBytes); it never writes back, controls, or schedules anything (RULES §A).
//
// It is unit-testable WITHOUT a GPU: point Path at a temp file containing NCCL
// log text (or leave the reader seam pluggable). The parsing is the shared
// parseNCCL (collectors_logs.go), already covered by fixture tests.

// NCCLLogCollector reads an NCCL log file and emits the NCCL watchdog/timeout
// events it observes, source-tagged SIGNAL_SOURCE_NCCL. It implements the
// read-only Collector interface.
type NCCLLogCollector struct {
	// Path is the NCCL log file to tail (read-only). Empty ⇒ NCCL stream
	// unavailable (degrade: no events, never fatal).
	Path string
	// Node stamps the node onto observation provenance (per-node DaemonSet).
	Node string

	// read is an optional reader seam for tests: when set it supplies the NCCL log
	// bytes directly, bypassing the file open. Nil ⇒ read Path from disk.
	read func() (io.Reader, error)
}

func (c NCCLLogCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_NCCL
}

// reader returns the NCCL log bytes for the window. The test seam wins; otherwise
// the file at Path is opened O_RDONLY, drained up to ncclMaxBytes, and closed. A
// missing/unreadable/empty file yields a nil reader so the parser degrades.
func (c NCCLLogCollector) reader() (io.Reader, error) {
	if c.read != nil {
		return c.read()
	}
	if c.Path == "" {
		return nil, nil
	}
	f, err := os.Open(c.Path) // O_RDONLY: read-only tail, never writes
	if err != nil {
		return nil, nil // unavailable ⇒ degrade, never fatal
	}
	defer func() { _ = f.Close() }()
	// Read the TAIL, not the head: NCCL watchdog/timeout lines accumulate at the
	// END of a long-running job's log. Seek to the last ncclMaxBytes so recent
	// events are always in-window (a head-bounded read would silently stop firing
	// nccl.timeout once the log grew past the cap).
	if fi, statErr := f.Stat(); statErr == nil && fi.Size() > ncclMaxBytes {
		if _, seekErr := f.Seek(fi.Size()-ncclMaxBytes, io.SeekStart); seekErr != nil {
			return nil, nil
		}
	}
	b, err := io.ReadAll(io.LimitReader(f, ncclMaxBytes))
	if err != nil || len(b) == 0 {
		return nil, nil
	}
	return bytes.NewReader(b), nil
}

// Collect reads and parses the NCCL log for the window into NCCL-sourced events.
// An unavailable/empty log is marked in provenance and yields no events — degrade,
// never fabricate, never a hard error (a missing NCCL log can never crash the
// daemon or affect a job).
func (c NCCLLogCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	obs := Observation{
		Source: c.Source(),
		Provenance: map[string]string{
			"source": "nccl-log",
			"node":   c.Node,
			"path":   c.Path,
		},
	}
	r, err := c.reader()
	if err != nil || r == nil {
		obs.Provenance["nccl"] = "unavailable"
		return obs, nil
	}
	obs.NcclEvents = parseNCCL(r, now)
	obs.Provenance["nccl"] = "read"
	return obs, nil
}
