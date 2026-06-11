//go:build !gpu

package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// tickClock is a deterministic advancing clock for the headless loop test.
type tickClock struct{ ns atomic.Int64 }

func (c *tickClock) now() time.Time {
	return time.Unix(0, c.ns.Add(int64(time.Second)))
}

// TestHeadlessRefreshNoClient proves the daemon's headless loop refreshes state
// with NO client attached: we start Run, never touch the API, and assert the
// refresh counter advances on its own. Off-critical-path: collection proceeds
// independently of any reader.
func TestHeadlessRefreshNoClient(t *testing.T) {
	clk := &tickClock{}
	d := NewDaemon(DaemonConfig{
		AgentID:    "headless",
		Window:     time.Minute,
		Collectors: DefaultCollectors("headless-node"),
		Policy:     DefaultCostPolicy(),
		Now:        clk.now,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { _ = d.Run(ctx, time.Millisecond); close(done) }()

	// With no client ever attaching, the state must populate and advance.
	deadline := time.After(3 * time.Second)
	var last uint64
	for {
		st := d.Snapshot()
		if st != nil {
			if st.Refreshes >= 3 && st.Refreshes > last {
				break // loop is refreshing headlessly
			}
			last = st.Refreshes
		}
		select {
		case <-deadline:
			t.Fatalf("headless loop did not refresh >=3 times without a client (got %d)", last)
		case <-time.After(2 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("daemon Run did not return after cancel")
	}

	// The last published state must be a complete, valid window.
	st := d.Snapshot()
	if st == nil || st.Window == nil || st.Window.Pack == nil {
		t.Fatalf("headless state incomplete")
	}
	if st.Window.Pack.ContractVersion != "v1" {
		t.Fatalf("headless window has wrong contract version")
	}
}

// TestRefreshImmediatePopulatesState proves Refresh works with no client and no
// loop (single-shot path used by the one-shot CLI mode).
func TestRefreshImmediatePopulatesState(t *testing.T) {
	d := NewDaemon(DaemonConfig{
		Window:     time.Minute,
		Collectors: DefaultCollectors("n"),
		Now:        fixedNow,
	})
	if d.Snapshot() != nil {
		t.Fatalf("state should be nil before first refresh")
	}
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	st := d.Snapshot()
	if st == nil || st.Refreshes != 1 {
		t.Fatalf("first refresh should publish state with Refreshes==1")
	}
}
