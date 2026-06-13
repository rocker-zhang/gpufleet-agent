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
//
// RefreshAt is the time of the SUCCESSFUL collection that produced THIS window —
// i.e. the data's own as-of time. A State is only ever published on a successful
// collection, so RefreshAt is the last-successful-collection time as long as
// this State is the live one. When a later collection fails, the daemon keeps
// THIS State in place (stale-but-valid) and RefreshAt stops advancing, so the
// freshness machinery (Daemon.Freshness) measures age against it.
type State struct {
	Window    *SignalWindow
	Cost      CostReport
	RefreshAt time.Time
	Refreshes uint64 // monotonically increasing refresh counter
}

// Freshness is the agent-side data-freshness verdict for the live State,
// computed at read time against the wall clock (TASK-0040). It answers "how long
// since the agent last SUCCESSFULLY scraped its sources, and is that too long?"
//
// This is the agent-MEASURABLE freshness (b): time since the last successful
// collection. It is NOT the upstream DCGM ~25-30s profiling sample window (a),
// which is a property of the exporter the agent cannot observe and only
// documents (agent CLAUDE.md §5d).
//
// When Stale is true the daemon is still serving the LAST-KNOWN values (never
// blanked, never fabricated — RULES §A/§B), but the consumer MUST present them
// as stale, not current. Reason carries the provenance of WHY (exporter
// unreachable / N consecutive collection failures). HasData is false before the
// very first successful collection — there is no last-known value to flag yet.
//
// NeverCollected (TASK-0041) is the distinct, MOST-stale case: the agent has
// NEVER once successfully scraped metrics (costState is nil) — e.g. the exporter
// was unreachable from startup. There is no last-known value to serve at all, so
// this is reported Stale=true (NeverCollected=true) with a populated Reason
// ("never collected: …") and an Age measured SINCE STARTUP — never a misleading
// age 0 that would read as "just collected / fresh". A never-collected agent is
// the most stale state and must NEVER be reported fresh (RULES §B).
type Freshness struct {
	HasData        bool          // a cost-bearing State has been published at least once
	NeverCollected bool          // no successful metrics scrape EVER (most stale; !HasData)
	CollectedAt    time.Time     // RefreshAt of the live State (last successful collection); zero when NeverCollected
	Age            time.Duration // now - CollectedAt when HasData; now - StartedAt when NeverCollected
	Stale          bool          // Age exceeded the staleness threshold, OR NeverCollected
	Reason         string        // provenance for Stale (empty when fresh)
	StalenessAfter time.Duration // the threshold in effect
	ConsecFails    uint64        // consecutive failed collections since the last success
}

// Daemon is the headless collection loop: it periodically reads every
// collector, normalizes to a SignalWindow, computes the standalone cost wedge,
// and publishes a new immutable State — whether or not any client is attached
// (RULES §A off-critical-path: a stalled/absent reader never blocks collection,
// and a collection error never affects a customer job). It NEVER writes back to
// any source.
type Daemon struct {
	agentID        string
	window         time.Duration
	stalenessAfter time.Duration // age beyond which the live State is marked stale
	collectors     []Collector
	policy         semantics.CostPolicy
	now            func() time.Time
	startedAt      time.Time // daemon construction time; ages the never-collected case (TASK-0041)

	mu    sync.RWMutex
	state *State // freshest published window (incl. log/XID-only cycles); serves /signals
	// costState is the last published State that actually carried per-device
	// METRICS (a device-bearing window) — the last time the agent SUCCESSFULLY
	// scraped the exporter for cost data. /cost + /healthz freshness is measured
	// against THIS, and /cost serves THIS, so a later log-only cycle (metrics
	// source down) never blanks the last-known device costs nor resets their age
	// (TASK-0040). Distinct from `state` because a non-metrics source keeping the
	// cycle alive must NOT make stale cost data look fresh.
	costState   *State
	errs        uint64 // cumulative count of refresh errors (collection never panics the node)
	consecFails uint64 // consecutive cycles WITHOUT a fresh metrics scrape, since the last good one
	lastReason  string // provenance of the most recent metrics-collection failure (stale reason)
}

// DaemonConfig configures a Daemon. Zero values get safe defaults.
type DaemonConfig struct {
	AgentID    string
	Window     time.Duration
	Collectors []Collector
	Policy     semantics.CostPolicy
	// StalenessAfter is the age (since the last SUCCESSFUL collection) beyond
	// which the live State is reported stale=true (TASK-0040). Zero ⇒ a safe
	// default of max(3×Window, 5s). The caller (cmd/agent --staleness-after)
	// normally derives it from the refresh interval, the natural collection
	// cadence, as max(3×interval, 5s).
	StalenessAfter time.Duration
	// Now overrides the clock for deterministic tests.
	Now func() time.Time
}

// defaultStalenessAfter derives a safe staleness threshold from a collection
// cadence d: three missed collections, but never less than a few seconds so a
// fast test/loop interval does not flap to stale on normal jitter. Exported-ish
// helper so cmd/agent derives the same default from its --interval.
func defaultStalenessAfter(d time.Duration) time.Duration {
	const floor = 5 * time.Second
	if v := 3 * d; v > floor {
		return v
	}
	return floor
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
	staleAfter := cfg.StalenessAfter
	if staleAfter <= 0 {
		staleAfter = defaultStalenessAfter(cfg.Window)
	}
	return &Daemon{
		agentID:        cfg.AgentID,
		window:         cfg.Window,
		stalenessAfter: staleAfter,
		collectors:     cfg.Collectors,
		policy:         policy,
		now:            cfg.Now,
		startedAt:      cfg.Now(),
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
// whole window. Refresh returns the first collector error seen (so callers can
// observe it) only when NO source produced data; otherwise it publishes the
// partial window and returns nil. A hard Normalize/cost error still leaves the
// previous State in place.
func (d *Daemon) Refresh(ctx context.Context) error {
	now := d.now()
	obs := make([]Observation, 0, len(d.collectors))
	// firstCollectErr captures the FIRST per-collector error of ANY kind this
	// round (soft/capped or otherwise) — every collector error is isolated and
	// counted; this just remembers the first to surface when no source answered.
	var firstCollectErr error
	softErrs := 0
	// metricsFresh: did the exporter answer with per-device METRICS this cycle?
	// True iff some collected observation carried DeviceWindows (the cost inputs).
	// A log/XID-only cycle (metrics source down) leaves this false, so cost
	// freshness ages even though the cycle still publishes a window (TASK-0040).
	metricsFresh := false
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
			if firstCollectErr == nil {
				firstCollectErr = err
			}
			continue
		}
		if len(o.DeviceWindows) > 0 {
			metricsFresh = true
		}
		obs = append(obs, o)
	}

	// If EVERY source failed this round, there is nothing new to normalize: keep
	// the previous State (stale-but-valid) and surface the first error so the
	// caller/loop counts it. We do NOT publish an empty window over a good one.
	// This is the canonical "exporter unreachable / collection failing" path that
	// drives staleness (TASK-0040): record it as a failed collection so age keeps
	// growing against the last good State and /cost can flag stale with a reason.
	if len(obs) == 0 && len(d.collectors) > 0 {
		d.recordMetricsFailure(firstCollectErr)
		if firstCollectErr != nil {
			return firstCollectErr
		}
		return nil
	}

	win, err := Normalize(d.agentID, now, d.window, obs)
	if err != nil {
		d.recordMetricsFailure(err)
		return err
	}
	// Record partial-collection provenance: some sources failed this round but the
	// window was still normalized and published from the others (degrade, not
	// drop). Non-adjudicating metadata only.
	if firstCollectErr != nil || len(obs) < len(d.collectors) {
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
		d.recordMetricsFailure(err)
		return err
	}

	newState := &State{Window: win, Cost: cost, RefreshAt: now}
	d.mu.Lock()
	prev := uint64(0)
	if d.state != nil {
		prev = d.state.Refreshes
	}
	newState.Refreshes = prev + 1
	// The freshest window (incl. a log/XID-only cycle) always advances — /signals
	// stays current.
	d.state = newState
	if metricsFresh {
		// The exporter answered with device metrics this cycle: cost data is fresh.
		// Advance the cost-bearing State and clear the staleness streak/reason.
		d.costState = newState
		d.consecFails = 0
		d.lastReason = ""
	} else {
		// The cycle published a window from a NON-metrics source (e.g. logs) but the
		// exporter gave NO device metrics — the last-known cost data is now older.
		// Keep the prior costState (retain device values, never blank) and age it,
		// so /cost flags stale rather than serving an empty window as live.
		d.errs++
		d.consecFails++
		d.lastReason = stalenessReason(d.consecFails, firstCollectErr, "no metrics scraped this cycle")
	}
	d.mu.Unlock()
	return nil
}

// recordMetricsFailure books one cycle that produced NO fresh metrics (TASK-0040):
// the exporter was unreachable or every source erred. It bumps the cumulative
// error counter AND the consecutive-failure streak and remembers a reason. It does
// NOT touch the published State — the previous good window stays in place
// (stale-but-valid), so age keeps growing against the last cost-bearing State and
// the reader flags stale without ever blanking or fabricating data (RULES §A/§B).
func (d *Daemon) recordMetricsFailure(err error) {
	d.mu.Lock()
	d.errs++
	d.consecFails++
	d.lastReason = stalenessReason(d.consecFails, err, "")
	d.mu.Unlock()
}

// stalenessReason renders the provenance string attached to a stale verdict: it
// names the consecutive-failure count and the underlying cause (e.g. the
// exporter being unreachable, or that no metrics were scraped), so the operator
// sees WHY the data is held stale rather than served as live. Deterministic
// given its inputs.
func stalenessReason(consecFails uint64, err error, fallback string) string {
	base := strconv.FormatUint(consecFails, 10) + " consecutive metrics-collection failure(s)"
	switch {
	case err != nil:
		return base + ": " + err.Error()
	case fallback != "":
		return base + ": " + fallback
	default:
		return base
	}
}

// neverCollectedReason renders the provenance for the never-collected verdict
// (TASK-0041): the agent has not yet succeeded even once. It folds in the last
// metrics-collection failure reason (exporter unreachable, etc.) when one was
// recorded, so the operator sees WHY no data exists, and otherwise states plainly
// that no collection has completed yet. Always non-empty.
func neverCollectedReason(lastReason string) string {
	const base = "never collected: no successful metrics scrape since startup"
	if lastReason != "" {
		return base + " (" + lastReason + ")"
	}
	return base
}

// Snapshot returns the latest published State (the freshest window, including a
// log/XID-only cycle), or nil if no refresh has run. This drives /signals. The
// returned pointer is immutable; callers must not mutate it.
func (d *Daemon) Snapshot() *State {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state
}

// CostSnapshot returns the last published State that carried per-device METRICS
// — the last-known cost-bearing window (TASK-0040). It is what /cost and /window
// serve, so a later metrics-less cycle (exporter down) never blanks the device
// values; the reader pairs this with Freshness() to flag it stale. nil if the
// exporter has never yet produced device metrics. Immutable; do not mutate.
func (d *Daemon) CostSnapshot() *State {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.costState
}

// Errs reports the cumulative count of refresh errors (off-path: errors are
// counted, never propagated to any customer job).
func (d *Daemon) Errs() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.errs
}

// Freshness computes the agent-side data-freshness verdict for the COST-bearing
// State against the wall clock NOW (TASK-0040). It measures age since the last
// time the exporter was SUCCESSFULLY scraped for device metrics and decides
// stale-vs-fresh by the configured threshold.
//
// It reads, never mutates: a stale verdict does not blank or alter the published
// State — the last-known cost-bearing window stays served, just explicitly
// flagged so a consumer never presents it as current (RULES §B). Before the first
// successful metrics scrape HasData is false (nothing to flag yet). When stale,
// Reason carries the provenance (exporter unreachable / N consecutive
// metrics-collection failures). Age is measured against the cost-bearing State,
// so a log/XID-only cycle (metrics source down) does NOT reset cost freshness.
func (d *Daemon) Freshness() Freshness {
	d.mu.RLock()
	st := d.costState
	consec := d.consecFails
	reason := d.lastReason
	d.mu.RUnlock()

	f := Freshness{StalenessAfter: d.stalenessAfter, ConsecFails: consec}
	if st == nil {
		// NEVER COLLECTED (TASK-0041): the agent has not once successfully scraped
		// metrics — e.g. the exporter was unreachable from startup. This is the MOST
		// stale state, not a fresh one: there is no last-known value to serve at all.
		// Report Stale=true + NeverCollected=true with a populated reason, and age
		// the data SINCE STARTUP (never a misleading 0 that would read as "just
		// collected"). RULES §B: never present never-collected as current/fresh.
		f.NeverCollected = true
		f.Stale = true
		age := d.now().Sub(d.startedAt)
		if age < 0 {
			age = 0 // clock-skew guard
		}
		f.Age = age
		f.Reason = neverCollectedReason(reason)
		return f
	}
	f.HasData = true
	f.CollectedAt = st.RefreshAt
	age := d.now().Sub(st.RefreshAt)
	if age < 0 {
		age = 0 // clock skew guard: never report a negative age
	}
	f.Age = age
	if age > d.stalenessAfter {
		f.Stale = true
		if reason == "" {
			// Age exceeded the threshold without a recorded failure cause (e.g. the
			// loop stopped refreshing). Still flag, with a generic provenance.
			reason = "data age " + age.Truncate(time.Millisecond).String() +
				" exceeds staleness threshold " + d.stalenessAfter.String()
		}
		f.Reason = reason
	}
	return f
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
