//go:build !gpu

package agent

import (
	"syscall"
	"testing"
	"time"
)

// TestRawNonblockReader_NeverBlocks is the TASK-0057 regression: reading a raw
// O_NONBLOCK fd with nothing buffered must return (0,nil) promptly, and a bounded
// drain over a never-filled nonblock fd must terminate on the DEADLINE rather
// than hang (the real /dev/kmsg hang was an *os.File* parking on the runtime
// poller at EAGAIN; rawNonblockReader bypasses that via syscall.Read).
func TestRawNonblockReader_NeverBlocks(t *testing.T) {
	var fds [2]int
	if err := syscall.Pipe2(fds[:], syscall.O_NONBLOCK); err != nil {
		t.Skipf("pipe2 unavailable: %v", err)
	}
	defer func() { _ = syscall.Close(fds[0]) }()
	defer func() { _ = syscall.Close(fds[1]) }()
	r := rawNonblockReader{fd: fds[0]}

	// Empty fd: a single Read returns (0,nil) immediately, never blocks.
	n, err := r.Read(make([]byte, 64))
	if n != 0 || err != nil {
		t.Fatalf("empty raw read = (%d,%v), want (0,nil)", n, err)
	}

	// Bounded drain over a never-filled nonblock fd must end on the deadline, not
	// hang. Run it under a watchdog so a regression fails instead of stalling CI.
	done := make(chan boundedReadResult, 1)
	go func() {
		_, res, _ := boundedReadStream(r, boundedReadConfig{Deadline: 50 * time.Millisecond})
		done <- res
	}()
	select {
	case res := <-done:
		if !res.HitDeadline {
			t.Fatalf("bounded drain did not hit deadline: %+v", res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("boundedReadStream HUNG on a nonblocking fd (TASK-0057 regression)")
	}

	// With data buffered, the reader returns it (then degrades to (0,nil)).
	if _, err := syscall.Write(fds[1], []byte("hello kmsg")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, _, _ := boundedReadStream(r, boundedReadConfig{Deadline: 50 * time.Millisecond})
	if string(got) != "hello kmsg" {
		t.Fatalf("raw drain = %q, want %q", got, "hello kmsg")
	}
}

// TestRawNonblockReader_ErrorPropagates: a non-EAGAIN/EINTR errno (here EBADF on a
// closed fd, standing in for the kmsg EPIPE ring-overrun case) propagates as a
// real error so the bounded drain ends cleanly (degrade, never hang) — TASK-0057.
func TestRawNonblockReader_ErrorPropagates(t *testing.T) {
	var fds [2]int
	if err := syscall.Pipe2(fds[:], syscall.O_NONBLOCK); err != nil {
		t.Skipf("pipe2 unavailable: %v", err)
	}
	_ = syscall.Close(fds[0])
	_ = syscall.Close(fds[1]) // both closed ⇒ reads on fds[0] return EBADF
	r := rawNonblockReader{fd: fds[0]}

	if n, err := r.Read(make([]byte, 16)); err == nil {
		t.Fatalf("closed-fd read = (%d,nil), want a propagated error", n)
	}
	// A bounded drain over an erroring reader must terminate immediately with no
	// bytes and no hang (it breaks on the error, not the deadline).
	got, _, _ := boundedReadStream(r, boundedReadConfig{Deadline: 2 * time.Second})
	if len(got) != 0 {
		t.Fatalf("expected 0 bytes on a hard read error, got %q", got)
	}
}
