package agent

import (
	"context"
	"errors"
	"strconv"
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

// softCollectErr reports whether a per-collector error is a SOFT/degradable
// failure — one source declining or rate-capping for this tick — as opposed to a
// hard error. A soft failure must NOT drop the whole cycle's Normalize: the
// agent keeps the OTHER sources' data that round and degrades only the missing
// one (RULES §A off-critical-path; TASK-0033 #3). Currently soft = the
// profiling-burst cap (ErrProfilingCapped); a single source failing is also
// treated as soft because every collector is independent and one source's
// failure must never blank the window. The classifier is centralized here so the
// set can grow without changing the loop.
func softCollectErr(err error) bool {
	return errors.Is(err, ErrProfilingCapped)
}

// Refresh performs one collection->normalize->cost cycle and publishes a new
// State. It is safe to call with no client attached. A collector or normalize
// error is recorded but never panics; the previous State is left in place so a
// transient source failure degrades to stale-but-valid, never to node impact.
//
// SOFT vs HARD collector errors (TASK-0033 #3): a soft/degradable collector
// error (e.g. ErrProfilingCapped, or any single source failing) does NOT abort
// the cycle — that source is skipped for this round and the OTHER sources are
// still normalized and published, so a capped/partial source never drops the
// whole window. Refresh returns the first soft error seen (so callers can
// observe it) only when NO source produced data; otherwise it publishes the
// partial window and returns nil. A hard Normalize/cost error still leaves the
// previous State in place.
func (d *Daemon) Refresh(ctx context.Context) error {
	now := d.now()
	obs := make([]Observation, 0, len(d.collectors))
	var firstSoftErr error
	softErrs := 0
	for _, c := range d.collectors {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		o, err := c.Collect(now, d.window)
		if err != nil {
			// Every collector error is isolated: count it and skip THIS source for
			// this round, keeping the others. A single source failing (soft or
			// otherwise) must never blank the window — the cycle proceeds with
			// whatever sources answered (degrade, not drop).
			d.mu.Lock()
			d.errs++
			d.mu.Unlock()
			if softCollectErr(err) {
				softErrs++
			}
			if firstSoftErr == nil {
				firstSoftErr = err
			}
			continue
		}
		obs = append(obs, o)
	}

	// If EVERY source failed this round, there is nothing new to normalize: keep
	// the previous State (stale-but-valid) and surface the first error so the
	// caller/loop counts it. We do NOT publish an empty window over a good one.
	if len(obs) == 0 && len(d.collectors) > 0 {
		if firstSoftErr != nil {
			return firstSoftErr
		}
		return nil
	}

	win, err := Normalize(d.agentID, now, d.window, obs)
	if err != nil {
		d.mu.Lock()
		d.errs++
		d.mu.Unlock()
		return err
	}
	// Record partial-collection provenance: some sources failed this round but the
	// window was still normalized and published from the others (degrade, not
	// drop). Non-adjudicating metadata only.
	if firstSoftErr != nil || len(obs) < len(d.collectors) {
		if win.Pack != nil {
			if win.Pack.Provenance == nil {
				win.Pack.Provenance = map[string]string{}
			}
			win.Pack.Provenance["agent.partial_collection"] = "true"
			win.Pack.Provenance["agent.failed_sources"] = strconv.Itoa(len(d.collectors) - len(obs))
			win.Pack.Provenance["agent.soft_capped_sources"] = strconv.Itoa(softErrs)
		}
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

// wireKill installs `kill` as the kill-switch on every collector that supports
// abort-on-demand (killWirable). The daemon derives `kill` from its Run context
// (ctx.Done()), so a shutdown / operator abort interrupts any in-flight,
// otherwise-bounded source read — e.g. the gpu-build /dev/kmsg drain — at the
// next chunk boundary. Collectors that cannot stall (the default fixture/mock
// sources) do not implement killWirable and are left untouched. This is the
// abort-on-demand control ON TOP of each source's deadline + byte cap, which
// remain the primary no-stall guarantee (RULES §A; TASK-0033 #1).
func (d *Daemon) wireKill(kill <-chan struct{}) {
	for _, c := range d.collectors {
		if w, ok := c.(killWirable); ok {
			w.SetKill(kill)
		}
	}
}

// Run drives the headless refresh loop on `interval` until ctx is cancelled. It
// refreshes once immediately so the State is populated even before the first
// tick, then on every tick — with NO client attached. Refresh errors are
// swallowed (counted via Errs); a failing source must never stop the loop or
// affect a job. Run returns ctx.Err() when cancelled.
//
// On entry it wires the kill-switch to ctx.Done() (TASK-0033 #1): when ctx is
// cancelled (SIGINT/SIGTERM shutdown or operator abort), any source mid-drain on
// a never-EOF stream is interrupted at the next chunk boundary instead of waiting
// out its deadline. The deadline + byte cap remain the primary no-stall bound.
func (d *Daemon) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = d.window
	}
	d.wireKill(ctx.Done())
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
