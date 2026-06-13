package agent

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"sync"
	"time"
)

// This file is the build-AGNOSTIC, no-GPU bounded-read core for the /dev/kmsg
// tail. /dev/kmsg is a NEVER-EOF stream: a naive io.ReadAll(io.LimitReader(f,
// MaxBytes)) blocks the collector goroutine until it reads the full cap or hangs
// forever — which would STALL THE NODE (RULES §A off-critical-path; module
// CLAUDE.md "resource guards + kill-switch so a probe can never stall a node").
//
// The real /dev/kmsg open (O_RDONLY|O_NONBLOCK) lives behind //go:build gpu in
// sources_gpu.go; the bounded-read LOGIC lives HERE, with NO build tag and an
// INJECTED reader, so it is unit-testable on the default build with a slow /
// never-ending reader and we can assert it NEVER blocks past the deadline.

// kmsgDefaultMaxBytes caps how many bytes the kmsg tail reads per window. It is a
// read-only bound: the collector drains at most this much of the currently
// available ring-buffer text, never the whole (never-EOF) stream.
const kmsgDefaultMaxBytes = 4 << 20 // 4 MiB

// kmsgDefaultDeadline bounds wall-clock time spent draining the stream per
// window. Even a perfectly non-blocking O_NONBLOCK reader could, in theory, keep
// returning bytes indefinitely if the buffer is being refilled as fast as we
// drain it; the deadline is the hard ceiling that guarantees the collector
// goroutine returns and can never stall the node.
const kmsgDefaultDeadline = 200 * time.Millisecond

// errKmsgKilled is returned internally when the kill-switch fired mid-read. It is
// never fatal: boundedReadStream returns whatever bytes were drained so far and
// the caller degrades on the marker, never crashes.
var errKmsgKilled = errors.New("agent: kmsg read kill-switch fired")

// boundedReadConfig configures one bounded, non-blocking drain of a never-EOF
// stream. Zero values get safe defaults so a caller can pass an empty config.
type boundedReadConfig struct {
	// MaxBytes caps total bytes drained. Zero ⇒ kmsgDefaultMaxBytes.
	MaxBytes int64
	// Deadline caps wall-clock time spent draining. Zero ⇒ kmsgDefaultDeadline.
	// This is the LINCHPIN: it guarantees the read returns even if the underlying
	// reader never blocks AND never ends (a refilling kmsg ring buffer).
	Deadline time.Duration
	// Kill, when non-nil, is the kill-switch: when it is closed (or sends), the
	// drain stops at the next chunk boundary and returns the bytes so far.
	Kill <-chan struct{}
	// chunk is the per-Read buffer size. Zero ⇒ 64 KiB. Exposed for tests.
	chunk int
	// now overrides the clock for deterministic tests. Nil ⇒ time.Now.
	now func() time.Time
}

// boundedReadResult reports how a bounded drain ended (for provenance + tests).
type boundedReadResult struct {
	// HitDeadline is true when the wall-clock deadline ended the drain.
	HitDeadline bool
	// HitCap is true when the byte cap ended the drain.
	HitCap bool
	// Killed is true when the kill-switch ended the drain.
	Killed bool
	// Bytes is how many bytes were drained.
	Bytes int64
	// Elapsed is the wall-clock time the drain took (clock-injected in tests).
	Elapsed time.Duration
}

// boundedReadStream drains a never-EOF stream in a strictly BOUNDED, non-blocking
// way: it reads in chunks until (a) EOF, (b) the byte cap, (c) the wall-clock
// deadline, or (d) the kill-switch — WHICHEVER COMES FIRST — and returns the
// bytes drained so far plus how it ended. It NEVER blocks past the deadline, so a
// collector goroutine calling it can never stall the node even if the underlying
// reader is slow or never-ending.
//
// CRITICAL CONTRACT: the underlying reader MUST be opened O_NONBLOCK (the real
// /dev/kmsg open in sources_gpu.go does this) so each Read returns promptly with
// whatever is buffered instead of blocking the OS thread. The deadline is the
// belt-and-suspenders ceiling for the pathological "non-blocking but endlessly
// refilling" case. A blocking reader that hangs INSIDE a single Read cannot be
// interrupted in pure Go without O_NONBLOCK; that is exactly why the open is
// non-blocking and why the test reader is non-blocking-but-never-ending.
func boundedReadStream(r io.Reader, cfg boundedReadConfig) ([]byte, boundedReadResult, error) {
	if r == nil {
		return nil, boundedReadResult{}, nil
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = kmsgDefaultMaxBytes
	}
	deadline := cfg.Deadline
	if deadline <= 0 {
		deadline = kmsgDefaultDeadline
	}
	chunk := cfg.chunk
	if chunk <= 0 {
		chunk = 64 << 10
	}
	clock := cfg.now
	if clock == nil {
		clock = time.Now
	}

	start := clock()
	var buf bytes.Buffer
	tmp := make([]byte, chunk)
	res := boundedReadResult{}

	for {
		// (d) kill-switch: stop at the chunk boundary, return what we have.
		if cfg.Kill != nil {
			select {
			case <-cfg.Kill:
				res.Killed = true
				res.Bytes = int64(buf.Len())
				res.Elapsed = clock().Sub(start)
				return buf.Bytes(), res, errKmsgKilled
			default:
			}
		}
		// (c) deadline: the hard ceiling that guarantees no-stall.
		if clock().Sub(start) >= deadline {
			res.HitDeadline = true
			break
		}
		// (b) byte cap: never drain more than the read-only bound.
		remaining := maxBytes - int64(buf.Len())
		if remaining <= 0 {
			res.HitCap = true
			break
		}
		readSize := int64(len(tmp))
		if remaining < readSize {
			readSize = remaining
		}
		n, err := r.Read(tmp[:readSize])
		if n > 0 {
			buf.Write(tmp[:n])
		}
		if int64(buf.Len()) >= maxBytes {
			res.HitCap = true
			break
		}
		if err != nil {
			// (a) EOF or any read error ends the drain cleanly: a normal end of the
			// currently-available buffer (O_NONBLOCK can surface EAGAIN as a 0,nil
			// read or an error depending on the reader; either way we stop). Read
			// errors degrade, never crash.
			break
		}
		// Guard against a non-blocking reader that returns (0, nil) forever: the
		// deadline check at the loop top will end it, but YIELD the processor first
		// so we don't busy-spin a core pinning the goroutine until the deadline. A
		// (0,nil) read is the EAGAIN-style "nothing buffered right now" case for an
		// O_NONBLOCK fd; runtime.Gosched lets the scheduler run other goroutines
		// (and the clock advance) instead of hot-looping. Bounded semantics are
		// unchanged: the deadline/cap/kill checks at the loop top still decide
		// termination on the next iteration.
		if n == 0 {
			runtime.Gosched()
			continue
		}
	}

	res.Bytes = int64(buf.Len())
	res.Elapsed = clock().Sub(start)
	return buf.Bytes(), res, nil
}

// boundedReadToReader is the convenience wrapper the kmsg/NCCL sources use: it
// drains the stream with the given config and returns an in-memory reader over
// the bounded bytes (so the parser sees a finite stream). A nil input yields a
// nil reader (the stream is treated as unavailable → degrade). It never returns
// the kill-switch error to the caller — a killed drain still yields its partial
// bytes so the parser degrades on whatever was observed, never crashing.
func boundedReadToReader(r io.Reader, cfg boundedReadConfig) (io.Reader, boundedReadResult) {
	if r == nil {
		return nil, boundedReadResult{}
	}
	b, res, _ := boundedReadStream(r, cfg)
	return bytes.NewReader(b), res
}

// killWirable is implemented by collectors/sources whose in-flight reads can be
// aborted on demand by a shared kill-switch channel. The daemon wires its own
// shutdown channel (derived from the Run context) into every collector that
// implements this, so an operator abort / shutdown can interrupt an in-progress
// /dev/kmsg drain at the next chunk boundary — the abort-on-demand control ON TOP
// of the deadline + byte cap, which remain the primary no-stall guarantee
// (RULES §A; module CLAUDE.md kill-switch rule; TASK-0033 #1).
//
// It is build-AGNOSTIC and unit-testable on the default build: SetKill simply
// stores the channel for the next read to observe. A nil channel is a no-op
// (kill-switch absent ⇒ deadline + cap still bound the read). Implementations
// must be safe for the daemon-loop / wire split, hence the killCell below.
type killWirable interface {
	// SetKill installs (or clears, with nil) the kill-switch channel used by
	// subsequent bounded reads. Idempotent and concurrency-safe.
	SetKill(kill <-chan struct{})
}

// killCell is the shared, mutable holder for a kill-switch channel. Sources such
// as kmsgLogSource embed it BY POINTER so a value-copy of the source (it is
// stored inside a LogEventCollector / []Collector by value) still observes the
// channel the daemon wired in — mirroring the selectedSource pattern used by
// MetricsChain. The zero value is usable and means "no kill-switch yet".
type killCell struct {
	mu   sync.RWMutex
	kill <-chan struct{}
}

// set installs the kill-switch channel (nil clears it). Safe on a nil receiver
// (a source constructed without a cell simply has no kill-switch).
func (k *killCell) set(kill <-chan struct{}) {
	if k == nil {
		return
	}
	k.mu.Lock()
	k.kill = kill
	k.mu.Unlock()
}

// get returns the currently-installed kill-switch channel (nil if none / nil
// receiver). Read each drain so a late wire is still observed.
func (k *killCell) get() <-chan struct{} {
	if k == nil {
		return nil
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.kill
}
