package agent

import (
	"context"
	"sync"
	"time"

	semantics "github.com/rocker-zhang/gpufleet-semantics"
)

// State is the agent's latest read-only snapshot: the most recent normalized
// SignalWindow plus its standalone cost report. It is refreshed by the daemon
// loop and read by the local API. It is immutable once published (a refresh
// swaps in a new *State), so readers never see a torn window.
type State struct {
	Window    *SignalWindow
	Cost      CostReport
	RefreshAt time.Time
	Refreshes uint64 // monotonically increasing refresh counter
}

// Daemon is the headless collection loop: it periodically reads every
// collector, normalizes to a SignalWindow, computes the standalone cost wedge,
// and publishes a new immutable State — whether or not any client is attached
// (RULES §A off-critical-path: a stalled/absent reader never blocks collection,
// and a collection error never affects a customer job). It NEVER writes back to
// any source.
type Daemon struct {
	agentID    string
	window     time.Duration
	collectors []Collector
	policy     semantics.CostPolicy
	now        func() time.Time

	mu    sync.RWMutex
	state *State
	errs  uint64 // count of refresh errors (collection never panics the node)
}

// DaemonConfig configures a Daemon. Zero values get safe defaults.
type DaemonConfig struct {
	AgentID    string
	Window     time.Duration
	Collectors []Collector
	Policy     semantics.CostPolicy
	// Now overrides the clock for deterministic tests.
	Now func() time.Time
}

// NewDaemon builds a Daemon. It does not start collecting; call Refresh once or
// Run to start the loop.
func NewDaemon(cfg DaemonConfig) *Daemon {
	if cfg.Window <= 0 {
		cfg.Window = 60 * time.Second
	}
	if cfg.AgentID == "" {
		cfg.AgentID = "gpufleet-agent"
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	policy := cfg.Policy
	if policy == (semantics.CostPolicy{}) {
		policy = semantics.DefaultCostPolicy()
	}
	return &Daemon{
		agentID:    cfg.AgentID,
		window:     cfg.Window,
		collectors: cfg.Collectors,
		policy:     policy,
		now:        cfg.Now,
	}
}

// Refresh performs one collection->normalize->cost cycle and publishes a new
// State. It is safe to call with no client attached. A collector or normalize
// error is returned but never panics; the previous State is left in place so a
// transient source failure degrades to stale-but-valid, never to node impact.
func (d *Daemon) Refresh(ctx context.Context) error {
	now := d.now()
	obs := make([]Observation, 0, len(d.collectors))
	for _, c := range d.collectors {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		o, err := c.Collect(now, d.window)
		if err != nil {
			d.mu.Lock()
			d.errs++
			d.mu.Unlock()
			return err
		}
		obs = append(obs, o)
	}

	win, err := Normalize(d.agentID, now, d.window, obs)
	if err != nil {
		d.mu.Lock()
		d.errs++
		d.mu.Unlock()
		return err
	}
	cost, err := win.CostWedge(d.policy)
	if err != nil {
		d.mu.Lock()
		d.errs++
		d.mu.Unlock()
		return err
	}

	d.mu.Lock()
	prev := uint64(0)
	if d.state != nil {
		prev = d.state.Refreshes
	}
	d.state = &State{Window: win, Cost: cost, RefreshAt: now, Refreshes: prev + 1}
	d.mu.Unlock()
	return nil
}

// Snapshot returns the latest published State, or nil if no refresh has run.
// The returned pointer is immutable; callers must not mutate it.
func (d *Daemon) Snapshot() *State {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state
}

// Errs reports the cumulative count of refresh errors (off-path: errors are
// counted, never propagated to any customer job).
func (d *Daemon) Errs() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.errs
}

// Run drives the headless refresh loop on `interval` until ctx is cancelled. It
// refreshes once immediately so the State is populated even before the first
// tick, then on every tick — with NO client attached. Refresh errors are
// swallowed (counted via Errs); a failing source must never stop the loop or
// affect a job. Run returns ctx.Err() when cancelled.
func (d *Daemon) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = d.window
	}
	_ = d.Refresh(ctx) // populate immediately; error is counted, not fatal
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_ = d.Refresh(ctx)
		}
	}
}
