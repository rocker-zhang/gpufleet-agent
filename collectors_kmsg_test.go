//go:build !gpu

package agent

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// neverEndingReader models the worst case of /dev/kmsg: a NON-BLOCKING stream
// that NEVER returns EOF — every Read returns bytes immediately and forever. This
// is exactly the shape that made the old io.ReadAll(io.LimitReader(f, MaxBytes))
// drain a full 4 MiB (or hang) and stall the collector goroutine. A bounded read
// MUST stop on the deadline / cap, never run forever.
type neverEndingReader struct {
	chunk []byte
	reads int
}

func (r *neverEndingReader) Read(p []byte) (int, error) {
	r.reads++
	n := copy(p, r.chunk)
	if n == 0 {
		// Always make progress so the only thing that can stop us is the bound.
		n = copy(p, []byte("x"))
	}
	return n, nil // NEVER EOF
}

// slowReader returns a small amount on each Read but pauses between Reads,
// modeling a non-blocking stream that trickles. It must still be bounded by the
// wall-clock deadline.
type slowReader struct {
	pause time.Duration
}

func (r *slowReader) Read(p []byte) (int, error) {
	time.Sleep(r.pause)
	n := copy(p, []byte("trickle\n"))
	return n, nil // NEVER EOF
}

// TestBoundedReadNeverBlocksPastDeadline is the core no-stall guarantee
// (TASK-0033 #1): with a NEVER-ending reader, boundedReadStream must return
// within (a small multiple of) the deadline and NEVER drain the full byte cap.
// We assert both the wall-clock bound and that it stopped on the deadline. This
// runs with NO hardware — the never-ending reader stands in for /dev/kmsg.
func TestBoundedReadNeverBlocksPastDeadline(t *testing.T) {
	deadline := 50 * time.Millisecond
	r := &neverEndingReader{chunk: []byte(strings.Repeat("a", 4096))}

	start := time.Now()
	done := make(chan struct{})
	var got []byte
	var res boundedReadResult
	go func() {
		got, res, _ = boundedReadStream(r, boundedReadConfig{
			MaxBytes: 64 << 20, // huge cap so ONLY the deadline can stop it
			Deadline: deadline,
		})
		close(done)
	}()

	// Hard ceiling: if the read is not bounded, this fails instead of hanging the
	// suite forever. We give the deadline generous slack (10x) for scheduler jitter
	// under -race, but it is still a finite bound that a never-ending read would blow.
	select {
	case <-done:
	case <-time.After(deadline * 10):
		t.Fatalf("bounded read BLOCKED past %v with a never-ending reader (no-stall guarantee violated)", deadline*10)
	}
	elapsed := time.Since(start)

	if !res.HitDeadline {
		t.Fatalf("bounded read should have stopped on the DEADLINE, got result %+v", res)
	}
	if res.HitCap {
		t.Fatalf("bounded read must NOT have drained the full byte cap from a never-ending reader: %+v", res)
	}
	if int64(len(got)) >= 64<<20 {
		t.Fatalf("bounded read drained the whole cap (%d bytes) — it would have stalled the node", len(got))
	}
	if elapsed > deadline*10 {
		t.Fatalf("bounded read took %v, far past the %v deadline", elapsed, deadline)
	}
	t.Logf("never-ending reader bounded: %d bytes in %v over %d reads (deadline=%v)", len(got), elapsed, r.reads, deadline)
}

// TestBoundedReadDeadlineDeterministic uses an INJECTED clock to assert the
// deadline logic is exact and hardware-free: with a clock that jumps past the
// deadline, the drain stops deterministically regardless of wall time.
func TestBoundedReadDeadlineDeterministic(t *testing.T) {
	clk := &manualClock{t: fixedNow()}
	r := &neverEndingReader{chunk: []byte("kmsgline\n")}

	calls := 0
	clock := func() time.Time {
		calls++
		// First call is the start stamp; advance past the deadline after a few
		// loop iterations so the drain ends on the injected deadline, not wall time.
		if calls > 3 {
			clk.advance(time.Second)
		}
		return clk.now()
	}
	_, res, err := boundedReadStream(r, boundedReadConfig{
		MaxBytes: 1 << 30,
		Deadline: 100 * time.Millisecond,
		now:      clock,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.HitDeadline {
		t.Fatalf("injected-clock deadline did not fire: %+v", res)
	}
}

// TestBoundedReadByteCap proves the read-only byte cap bounds a never-ending
// reader even with a generous deadline: the drain stops at MaxBytes.
func TestBoundedReadByteCap(t *testing.T) {
	r := &neverEndingReader{chunk: []byte(strings.Repeat("b", 8192))}
	got, res, err := boundedReadStream(r, boundedReadConfig{
		MaxBytes: 16 << 10, // 16 KiB cap
		Deadline: 10 * time.Second,
		chunk:    4 << 10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.HitCap {
		t.Fatalf("byte cap should have ended the drain: %+v", res)
	}
	if int64(len(got)) > 16<<10 {
		t.Fatalf("drain exceeded the byte cap: %d > %d", len(got), 16<<10)
	}
}

// TestBoundedReadKillSwitch proves the kill-switch aborts an in-progress drain of
// a never-ending reader and returns the partial bytes (degrade, never crash).
func TestBoundedReadKillSwitch(t *testing.T) {
	kill := make(chan struct{})
	close(kill) // already fired

	r := &neverEndingReader{chunk: []byte("data\n")}
	got, res, err := boundedReadStream(r, boundedReadConfig{
		MaxBytes: 1 << 30,
		Deadline: 10 * time.Second,
		Kill:     kill,
	})
	if !errors.Is(err, errKmsgKilled) {
		t.Fatalf("kill-switch should surface errKmsgKilled internally, got %v", err)
	}
	if !res.Killed {
		t.Fatalf("kill-switch result not recorded: %+v", res)
	}
	// boundedReadToReader must NOT propagate the kill error and must degrade on
	// the partial bytes (here zero, since the switch fired before any read).
	rd, res2 := boundedReadToReader(r, boundedReadConfig{Kill: kill, Deadline: time.Second})
	if rd == nil {
		t.Fatalf("boundedReadToReader must still return a reader after a kill")
	}
	if !res2.Killed {
		t.Fatalf("boundedReadToReader must record the kill: %+v", res2)
	}
	_ = got
}

// TestBoundedReadKillAbortsPromptly proves the kill-switch ABORTS an in-flight
// drain of a NEVER-ending reader PROMPTLY — long before the (here very large)
// deadline — when the kill channel is closed concurrently. This is the
// abort-on-demand control on top of the deadline/cap (TASK-0033 #1): a closed
// kill channel (as ctx.Done() becomes on shutdown) returns the read without
// waiting out the deadline. No hardware: the never-ending reader stands in for
// /dev/kmsg.
func TestBoundedReadKillAbortsPromptly(t *testing.T) {
	kill := make(chan struct{})
	r := &neverEndingReader{chunk: []byte(strings.Repeat("a", 4096))}

	start := time.Now()
	done := make(chan boundedReadResult, 1)
	go func() {
		_, res, _ := boundedReadStream(r, boundedReadConfig{
			MaxBytes: 64 << 20,
			Deadline: 30 * time.Second, // huge: ONLY the kill can stop this quickly
			Kill:     kill,
			chunk:    1 << 10,
		})
		done <- res
	}()

	// Fire the kill-switch shortly after the drain starts.
	time.Sleep(5 * time.Millisecond)
	close(kill)

	select {
	case res := <-done:
		if !res.Killed {
			t.Fatalf("drain should have ended on the kill-switch, got %+v", res)
		}
		if res.HitDeadline {
			t.Fatalf("kill must abort BEFORE the deadline, got %+v", res)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("kill-switch did not abort promptly: took %v", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("kill-switch did NOT interrupt the never-ending drain (abort-on-demand violated)")
	}
}

// killWirableSource is a build-agnostic LogSource that records the kill channel
// the daemon wires into it and, on Kmsg(), drains a never-ending reader through
// boundedReadStream honoring that channel — proving the daemon→collector→source
// kill wiring works end to end with NO hardware.
type killWirableSource struct {
	cell    *killCell
	reader  *neverEndingReader
	drained chan boundedReadResult
}

func newKillWirableSource() *killWirableSource {
	return &killWirableSource{
		cell:    &killCell{},
		reader:  &neverEndingReader{chunk: []byte("kmsgline\n")},
		drained: make(chan boundedReadResult, 1),
	}
}

func (s *killWirableSource) SetKill(kill <-chan struct{}) { s.cell.set(kill) }

func (s *killWirableSource) Kmsg() (io.Reader, error) {
	r, res := boundedReadToReader(s.reader, boundedReadConfig{
		MaxBytes: 64 << 20,
		Deadline: 30 * time.Second, // only the wired kill can end this promptly
		Kill:     s.cell.get(),
		chunk:    1 << 10,
	})
	select {
	case s.drained <- res:
	default:
	}
	return r, nil
}

func (s *killWirableSource) NCCL() (io.Reader, error) { return nil, nil }

// TestDaemonWiresKillSwitchToSource proves TASK-0033 #1 end to end: the daemon
// derives a kill channel from its Run context and wires it (via
// LogEventCollector → killWirable source) so cancelling the context aborts an
// in-flight, otherwise-deadline-bound /dev/kmsg-shaped drain PROMPTLY. Fully
// build-agnostic and hardware-free.
func TestDaemonWiresKillSwitchToSource(t *testing.T) {
	src := newKillWirableSource()
	d := NewDaemon(DaemonConfig{
		AgentID:    "kill-wire",
		Window:     time.Minute,
		Collectors: []Collector{LogEventCollector{Src: src, Node: "n"}},
		Now:        fixedNow,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// wireKill is what Run calls on entry; exercise it directly so the test does
	// not depend on the ticker loop.
	d.wireKill(ctx.Done())
	if src.cell.get() == nil {
		t.Fatalf("daemon did not wire the kill channel into the killWirable source")
	}

	// Run one Refresh concurrently (it will block in the source's never-ending
	// drain until the kill fires), then cancel the context to abort it.
	refreshErr := make(chan error, 1)
	go func() { refreshErr <- d.Refresh(ctx) }()

	time.Sleep(5 * time.Millisecond)
	cancel() // closes ctx.Done() → the wired kill channel

	select {
	case res := <-src.drained:
		if !res.Killed {
			t.Fatalf("context cancel did not abort the drain via the wired kill-switch: %+v", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("context cancel did NOT interrupt the in-flight drain (kill wiring broken)")
	}
	<-refreshErr // Refresh returns after the aborted drain; degrade, never hang
}

// TestDaemonWireKillIgnoresNonWirable proves wiring is a safe no-op for
// collectors that cannot stall (the default fixture/mock sources do not
// implement killWirable) — the default !gpu build is unchanged.
func TestDaemonWireKillIgnoresNonWirable(t *testing.T) {
	d := NewDaemon(DaemonConfig{
		AgentID:    "no-wire",
		Window:     time.Minute,
		Collectors: DefaultCollectors("n"), // mock sources, none killWirable
		Now:        fixedNow,
	})
	kill := make(chan struct{})
	d.wireKill(kill) // must not panic and must leave the mock collectors usable
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("default collectors must refresh fine after a no-op kill wire: %v", err)
	}
}

// TestBoundedReadSlowReader proves a trickling, never-ending reader is still
// bounded by the deadline.
func TestBoundedReadSlowReader(t *testing.T) {
	start := time.Now()
	_, res, _ := boundedReadStream(&slowReader{pause: 2 * time.Millisecond}, boundedReadConfig{
		MaxBytes: 1 << 30,
		Deadline: 40 * time.Millisecond,
	})
	if time.Since(start) > 400*time.Millisecond {
		t.Fatalf("slow never-ending reader was not bounded by the deadline")
	}
	if !res.HitDeadline {
		t.Fatalf("slow reader drain should end on the deadline: %+v", res)
	}
}

// TestBoundedReadFiniteStreamReadsAll proves the bound does NOT truncate a normal
// finite stream that ends within the cap/deadline: it reads everything to EOF.
func TestBoundedReadFiniteStreamReadsAll(t *testing.T) {
	const payload = "line-1\nline-2\nline-3\n"
	got, res, err := boundedReadStream(io.Reader(strings.NewReader(payload)), boundedReadConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("finite stream truncated: got %q want %q", got, payload)
	}
	if res.HitDeadline || res.HitCap || res.Killed {
		t.Fatalf("finite stream should end on EOF, not a bound: %+v", res)
	}
}

// TestBoundedReadNilReader proves a nil reader degrades to nil (stream
// unavailable), never panics.
func TestBoundedReadNilReader(t *testing.T) {
	if r, _ := boundedReadToReader(nil, boundedReadConfig{}); r != nil {
		t.Fatalf("nil reader must yield a nil reader (unavailable ⇒ degrade)")
	}
}
