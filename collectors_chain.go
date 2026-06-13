package agent

import (
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
}

func (c MetricsChain) Source() gpufleetv1.SignalSource {
	if c.Primary != nil {
		return c.Primary.Source()
	}
	if c.Fallback != nil {
		return c.Fallback.Source()
	}
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS
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
		// primary observation so the normalizer degrades every field.
		empty := Observation{Source: c.Source(), Provenance: map[string]string{}}
		markChainProvenance(&empty, "primary-empty", reasonFor(primaryErr), c.Source())
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
	markChainProvenance(&fobs, "fallback", reasonFor(primaryErr), c.Source())
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
