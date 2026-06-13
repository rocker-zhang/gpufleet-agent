package agent

import (
	"sync"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file implements the SOURCE-SELECTION DEGRADATION CHAIN (D-0009): for
// metrics, prefer the customer's EXISTING Prometheus (zero new scrape load); on
// Prometheus error or absent data, fall back to a local DCGM-exporter scrape; on
// fields still missing, the normalizer marks+degrades (degrade, never fabricate;
// never compute on absent data). The chain is read-only and off-critical-path: a
// source failure is isolated (recorded in provenance) and never crashes the
// daemon or affects a customer job.

// MetricsSource is the subset of Collector the chain composes (Prometheus,
// DCGM). It is just the Collector interface, named for intent.
type MetricsSource = Collector

// MetricsChain selects a metrics source per the degradation chain. It reports
// the PROMETHEUS source class (the preferred/default source); when it falls back
// to DCGM the fallback is recorded in provenance so provenance stays honest, but
// the chain still emits one SignalSource for the >=2-signal independence
// accounting. Prefer wiring Prometheus + DCGM as SEPARATE collectors when you
// want both to count as independent sources; use MetricsChain when you want a
// single "best available metrics" source with automatic fallback.
type MetricsChain struct {
	// Primary is the preferred metrics source (the existing Prometheus).
	Primary Collector
	// Fallback is used only when Primary errors or returns no device data.
	Fallback Collector
	// FieldFallback, when true, ALSO consults Fallback when Primary returned data
	// but left key fields (peak) missing, merging the fallback's data in. Default
	// false: fallback is used only when Primary wholly fails/empties.
	FieldFallback bool

	// selected records the source ACTUALLY chosen by the most recent Collect, so
	// Source() reflects the real provenance after a fallback (TASK-0033 #2). It is
	// a pointer so a MetricsChain copied by value into a []Collector still shares
	// one selection cell; nil ⇒ no Collect has run yet, fall back to the
	// declared-preference source. Guarded by mu for the concurrent
	// daemon-loop / API-read split.
	selected *selectedSource
}

// selectedSource is the shared, mutable cell recording the source a MetricsChain
// last actually selected. Held by pointer so value-copies of the chain (it is
// stored in a []Collector by value) observe the same selection.
type selectedSource struct {
	mu  sync.RWMutex
	src gpufleetv1.SignalSource
	set bool
}

// declaredSource is the chain's preference order BEFORE any Collect runs: Primary
// if present, else Fallback, else PROMETHEUS. Source() returns the actually
// selected source once Collect has run, and this declared default before then.
func (c MetricsChain) declaredSource() gpufleetv1.SignalSource {
	if c.Primary != nil {
		return c.Primary.Source()
	}
	if c.Fallback != nil {
		return c.Fallback.Source()
	}
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS
}

// Source reports the source the chain's signals will be tagged with. After a
// Collect it is the source that ACTUALLY answered (DCGM when Primary fell back),
// matching the emitted Observation.Source so the >=2-signal independence
// accounting is judged on the real provenance, not the declared preference
// (TASK-0033 #2; TASK-0018 independence-by-source). Before the first Collect it
// is the declared-preference source.
func (c MetricsChain) Source() gpufleetv1.SignalSource {
	if c.selected != nil {
		c.selected.mu.RLock()
		set, src := c.selected.set, c.selected.src
		c.selected.mu.RUnlock()
		if set {
			return src
		}
	}
	return c.declaredSource()
}

// recordSelected stores the actually-selected source for Source() to report.
// Safe to call with a nil receiver cell (no-op when the chain was constructed
// without the shared cell, e.g. a zero-value literal — Source() then degrades to
// the declared default, preserving the prior behavior).
func (c MetricsChain) recordSelected(src gpufleetv1.SignalSource) {
	if c.selected == nil {
		return
	}
	c.selected.mu.Lock()
	c.selected.src, c.selected.set = src, true
	c.selected.mu.Unlock()
}

// NewMetricsChain builds a MetricsChain whose Source() reflects the actually
// selected source after a fallback. Prefer this over a struct literal when the
// chain is wired into a daemon, so Source() and Observation.Source agree even
// after Primary falls back to Fallback.
func NewMetricsChain(primary, fallback Collector) MetricsChain {
	return MetricsChain{Primary: primary, Fallback: fallback, selected: &selectedSource{}}
}

// Collect runs the degradation chain. It tries Primary; if Primary errors or
// returns no device windows, it falls back to Fallback. The returned
// observation's provenance records which source actually answered and whether a
// fallback occurred. If BOTH fail, the Fallback's error is returned so the
// daemon counts+degrades.
func (c MetricsChain) Collect(now time.Time, window time.Duration) (Observation, error) {
	var primaryErr error
	if c.Primary != nil {
		obs, err := c.Primary.Collect(now, window)
		if err == nil && len(obs.DeviceWindows) > 0 {
			if c.FieldFallback && c.Fallback != nil && missingPeak(obs) {
				obs = c.mergeFallback(obs, now, window)
			}
			// Primary answered: the selected source is Primary's, and that is what
			// Observation.Source already carries.
			c.recordSelected(c.Primary.Source())
			markChainProvenance(&obs, "primary", "", c.Primary.Source())
			return obs, nil
		}
		primaryErr = err
	}

	if c.Fallback == nil {
		if primaryErr != nil {
			return Observation{}, primaryErr
		}
		// Primary returned no data and there is no fallback: emit the empty
		// primary observation so the normalizer degrades every field. The selected
		// source is the (only) declared source.
		sel := c.declaredSource()
		c.recordSelected(sel)
		empty := Observation{Source: sel, Provenance: map[string]string{}}
		markChainProvenance(&empty, "primary-empty", reasonFor(primaryErr), sel)
		return empty, nil
	}

	fobs, ferr := c.Fallback.Collect(now, window)
	if ferr != nil {
		// Both failed. Return the fallback error (daemon counts+degrades). Provide
		// the most informative error.
		if primaryErr != nil {
			return Observation{}, primaryErr
		}
		return Observation{}, ferr
	}
	// Fallback answered: the ACTUALLY-selected source is the Fallback's. Record it
	// so Source() reports DCGM (matching fobs.Source), not the declared Prometheus
	// preference — keeping independence-by-source honest (TASK-0033 #2).
	fsrc := c.Fallback.Source()
	c.recordSelected(fsrc)
	markChainProvenance(&fobs, "fallback", reasonFor(primaryErr), fsrc)
	return fobs, nil
}

// mergeFallback augments a primary observation with fallback device data for the
// fields the primary left missing (used when FieldFallback is on). It never
// overwrites a known primary value (degrade-aware additive merge).
func (c MetricsChain) mergeFallback(primary Observation, now time.Time, window time.Duration) Observation {
	fobs, err := c.Fallback.Collect(now, window)
	if err != nil {
		// Fallback unavailable: keep primary as-is; normalizer degrades the gap.
		if primary.Provenance == nil {
			primary.Provenance = map[string]string{}
		}
		primary.Provenance["chain.field_fallback"] = "unavailable:" + reasonFor(err)
		return primary
	}
	byUUID := map[string]int{}
	for i, dw := range primary.DeviceWindows {
		byUUID[dw.UUID] = i
	}
	for _, fdw := range fobs.DeviceWindows {
		if idx, ok := byUUID[fdw.UUID]; ok {
			merged := primary.DeviceWindows[idx]
			mergeDeviceWindow(&merged, fdw)
			primary.DeviceWindows[idx] = merged
		} else {
			primary.DeviceWindows = append(primary.DeviceWindows, fdw)
		}
	}
	primary.DcgmSeries = append(primary.DcgmSeries, fobs.DcgmSeries...)
	if primary.Provenance == nil {
		primary.Provenance = map[string]string{}
	}
	primary.Provenance["chain.field_fallback"] = "merged-dcgm"
	return primary
}

// missingPeak reports whether any device window lacks a known peak (the field
// most often missing from Prometheus that DCGM/spec must complete).
func missingPeak(obs Observation) bool {
	for _, dw := range obs.DeviceWindows {
		if !dw.PeakFLOPSKnown {
			return true
		}
	}
	return false
}

func markChainProvenance(obs *Observation, selected, reason string, src gpufleetv1.SignalSource) {
	if obs.Provenance == nil {
		obs.Provenance = map[string]string{}
	}
	obs.Provenance["chain.selected"] = selected
	obs.Provenance["chain.source"] = sourceShort(src)
	if reason != "" {
		obs.Provenance["chain.fallback_reason"] = reason
	}
}

func reasonFor(err error) string {
	if err == nil {
		return "primary returned no device data"
	}
	return err.Error()
}
