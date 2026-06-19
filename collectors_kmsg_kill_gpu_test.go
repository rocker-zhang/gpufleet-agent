//go:build gpu

package agent

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestKmsgLogSource_RealPathBounded exercises the REAL gpu-build kmsg path
// (syscall.Open(O_RDONLY|O_NONBLOCK) + rawNonblockReader + boundedReadStream) end
// to end over a real fd (a FIFO with no writer ⇒ the reader opens and the drain
// must terminate), asserting BOTH bounding mechanisms return promptly rather than
// parking forever: (a) a fired kill-switch, (b) the wall-clock deadline. This is
// the gpu-tagged complement to the !gpu rawNonblockReader test (TASK-0056/0057
// carry: the real Kmsg() wiring + kill path were previously validated only by
// composition / real hardware).
func TestKmsgLogSource_RealPathBounded(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "kmsg.fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}

	// (a) kill already fired ⇒ the first loop-top check must abort the drain.
	t.Run("kill", func(t *testing.T) {
		src := newKmsgLogSource(fifo, "")
		kill := make(chan struct{})
		close(kill)
		src.SetKill(kill)
		assertReturnsWithin(t, src, 3*time.Second, "fired kill-switch")
	})

	// (b) no kill, a tight deadline ⇒ the drain must end on the deadline.
	t.Run("deadline", func(t *testing.T) {
		src := newKmsgLogSource(fifo, "")
		src.Deadline = 50 * time.Millisecond
		assertReturnsWithin(t, src, 3*time.Second, "deadline")
	})
}

func assertReturnsWithin(t *testing.T, src *kmsgLogSource, d time.Duration, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_, _ = src.Kmsg() // must return promptly, never hang on the never-EOF fd
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("kmsgLogSource.Kmsg hung past %s despite %s (gpu kill/deadline regression)", d, what)
	}
}
