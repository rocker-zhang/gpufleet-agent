package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file is the STATIC DEVICE-SPEC fallback (TASK-0038). On a vanilla
// dcgm-exporter + Prometheus box the cost-rate series
// (gpufleet_device_cost_usd_per_hour) and the tensor-peak field
// (DCGM_FI_DEV_TENSOR_PEAK_FLOPS) DO NOT EXIST, so $/hr and MFU silently degrade
// to 0. $/hr is inherently an operator PRICE input; peak-FLOPS for a known GPU
// model is a static spec. So a static device-spec is the correct design.
//
// degrade-not-fabricate (RULES §B) is preserved: the static spec is an EXPLICIT
// operator input, so using it is NOT fabrication — but every spec-sourced value
// is STAMPED with provenance (cost.source=static-spec / peak.source=static-spec)
// so the origin stays auditable. A REAL series, when present, ALWAYS wins over
// the spec (the spec only fills a gap a real source left). When NEITHER a real
// series NOR a spec covers a field, the field still DEGRADES (it is never
// invented). This is build-AGNOSTIC: it touches only the scalar DeviceWindow
// aggregates, no NVML, so it compiles in the default !gpu build.

// DeviceSpecEntry is one operator-supplied static spec for a GPU model or an
// individual device UUID: the advertised peak (TFLOP/s) and the $/hour price.
// A zero/omitted field means "not specified" — it fills nothing and the field
// keeps degrading (degrade, never fabricate). PeakTFLOPS is in TFLOP/s for
// operator ergonomics (e.g. an A10 is 125 TFLOP/s tensor); it is converted to
// FLOP/s internally to match DeviceWindow.PeakFLOPS.
type DeviceSpecEntry struct {
	PeakTFLOPS  float64 `json:"peak_tflops"`
	CostPerHour float64 `json:"cost_usd_per_hour"`
}

func (e DeviceSpecEntry) hasPeak() bool { return e.PeakTFLOPS > 0 }
func (e DeviceSpecEntry) hasCost() bool { return e.CostPerHour > 0 }
func (e DeviceSpecEntry) empty() bool   { return !e.hasPeak() && !e.hasCost() }

// DeviceSpec is the operator-supplied static device-spec table. A device is
// matched by UUID FIRST (most specific), then by GPU model name. Both maps are
// optional. The zero value matches nothing and fills nothing (pure passthrough),
// keeping the no-spec path byte-for-byte backward compatible.
type DeviceSpec struct {
	// ByUUID maps an exact device UUID to its spec (most specific; wins over model).
	ByUUID map[string]DeviceSpecEntry `json:"by_uuid"`
	// ByModel maps a GPU model name (the DCGM `modelName` label, e.g. "NVIDIA A10")
	// to its spec. Matching is case-insensitive and tolerant of an "NVIDIA " prefix
	// so an operator may write either "NVIDIA A10" or "A10".
	ByModel map[string]DeviceSpecEntry `json:"by_model"`
}

// WildcardModel is the ByModel key matched as a LAST RESORT for any device no
// UUID/model entry named — the homogeneous-box convenience behind the quick
// --peak-tflops / --cost-usd-per-hour shortcuts. Named entries always win.
const WildcardModel = "*"

// Empty reports whether the spec has no entries at all (nothing to fill).
func (s DeviceSpec) Empty() bool { return len(s.ByUUID) == 0 && len(s.ByModel) == 0 }

// normModel canonicalizes a GPU model name for tolerant matching: lower-cased,
// trimmed, with a leading "nvidia " vendor prefix dropped. So "NVIDIA A10",
// "nvidia a10", and "A10" all match the same entry.
func normModel(m string) string {
	m = strings.ToLower(strings.TrimSpace(m))
	m = strings.TrimPrefix(m, "nvidia ")
	return strings.TrimSpace(m)
}

// lookup returns the spec entry for a device, UUID first then model (tolerant),
// and whether a match was found. UUID is the most specific key and wins.
func (s DeviceSpec) lookup(uuid, model string) (DeviceSpecEntry, bool) {
	if e, ok := s.ByUUID[uuid]; ok && !e.empty() {
		return e, true
	}
	if model != "" {
		// Exact first, then tolerant (normalized) match.
		if e, ok := s.ByModel[model]; ok && !e.empty() {
			return e, true
		}
		want := normModel(model)
		for k, e := range s.ByModel {
			if k == WildcardModel {
				continue // wildcard is a last resort, handled below
			}
			if normModel(k) == want && !e.empty() {
				return e, true
			}
		}
	}
	// Last resort: the homogeneous-box wildcard, applied to any device no named
	// UUID/model entry matched. Named entries above always win.
	if e, ok := s.ByModel[WildcardModel]; ok && !e.empty() {
		return e, true
	}
	return DeviceSpecEntry{}, false
}

// deviceSpecFileShape is the JSON the operator authors. The common case — a flat
// per-model map like {"NVIDIA A10": {"peak_tflops": 125, "cost_usd_per_hour":
// 1.20}} — is accepted directly. An explicit {"by_uuid": {...}, "by_model":
// {...}} object is also accepted so UUID-pinned specs are expressible. Both can
// be combined: any top-level key that is NOT "by_uuid"/"by_model" is treated as
// a model entry, so the flat form and the explicit form may coexist.
type deviceSpecFileShape map[string]json.RawMessage

// LoadDeviceSpec reads a device-spec JSON file into a DeviceSpec. It accepts
// either the flat per-model shape or the explicit {by_uuid,by_model} shape (see
// deviceSpecFileShape). A missing path is an error (the operator asked for a
// spec that is not there); an empty/whitespace path yields an empty spec and no
// error (no spec requested). It is read-only: O_RDONLY, never writes back.
func LoadDeviceSpec(path string) (DeviceSpec, error) {
	if strings.TrimSpace(path) == "" {
		return DeviceSpec{}, nil
	}
	b, err := os.ReadFile(path) // O_RDONLY read; never writes back (RULES §A)
	if err != nil {
		return DeviceSpec{}, fmt.Errorf("agent: read device-spec file %q: %w", path, err)
	}
	return ParseDeviceSpec(b)
}

// ParseDeviceSpec parses device-spec JSON bytes into a DeviceSpec, accepting the
// flat per-model shape and/or the explicit {by_uuid,by_model} shape.
func ParseDeviceSpec(b []byte) (DeviceSpec, error) {
	var raw deviceSpecFileShape
	if err := json.Unmarshal(b, &raw); err != nil {
		return DeviceSpec{}, fmt.Errorf("agent: parse device-spec json: %w", err)
	}
	spec := DeviceSpec{
		ByUUID:  map[string]DeviceSpecEntry{},
		ByModel: map[string]DeviceSpecEntry{},
	}
	for k, v := range raw {
		switch k {
		case "by_uuid":
			m := map[string]DeviceSpecEntry{}
			if err := json.Unmarshal(v, &m); err != nil {
				return DeviceSpec{}, fmt.Errorf("agent: parse device-spec by_uuid: %w", err)
			}
			for uuid, e := range m {
				spec.ByUUID[uuid] = e
			}
		case "by_model":
			m := map[string]DeviceSpecEntry{}
			if err := json.Unmarshal(v, &m); err != nil {
				return DeviceSpec{}, fmt.Errorf("agent: parse device-spec by_model: %w", err)
			}
			for model, e := range m {
				spec.ByModel[model] = e
			}
		default:
			// Flat per-model entry (the common A10 case).
			var e DeviceSpecEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return DeviceSpec{}, fmt.Errorf("agent: parse device-spec model %q: %w", k, err)
			}
			spec.ByModel[k] = e
		}
	}
	return spec, nil
}

// Provenance keys stamped when a static-spec value fills a gap, so the origin of
// a peak/cost number is always auditable (RULES §B): a reviewer can tell a
// spec-sourced $/hr from a real metered one.
const (
	specSourceValue   = "static-spec"
	provKeyPeakSource     = "spec.peak.source"
	provKeyCostSource     = "spec.cost.source"
	provKeyAchievedSource = "spec.achieved_flops.source"
	provKeySpecFilled     = "spec.filled_devices"
)

// SpecFillCollector wraps an inner metrics collector and FILLS missing per-device
// peak/cost from the operator's static DeviceSpec. The real series ALWAYS wins:
// a field already marked known by the inner source is left untouched; the spec
// only fills a gap. Every value it fills is stamped with provenance
// (peak.source=static-spec / cost.source=static-spec) so the origin stays
// auditable. When the spec is empty it is a transparent passthrough (no-spec
// behavior unchanged). It is read-only and build-agnostic (scalar fields only).
type SpecFillCollector struct {
	// Inner is the wrapped metrics collector (the real chain or any Collector).
	Inner Collector
	// Spec is the operator's static device-spec table. Empty ⇒ passthrough.
	Spec DeviceSpec
}

// Source delegates to the inner collector so independence-by-source accounting
// is unchanged: spec-fill adds no new SignalSource, it only completes fields the
// inner source already attributed to a device.
func (c SpecFillCollector) Source() gpufleetv1.SignalSource { return c.Inner.Source() }

// Collect runs the inner collector then fills missing peak/cost from the spec,
// stamping provenance. A spec match for a device whose field the inner source
// ALREADY supplied is ignored (real wins). Devices the inner source never saw
// are NOT invented from a model spec (a model spec needs a real device to attach
// to); a UUID-pinned spec, however, can introduce a device the inner source left
// out only when that device was otherwise observed — to stay degrade-not-
// fabricate we only fill windows the inner source emitted.
func (c SpecFillCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	obs, err := c.Inner.Collect(now, window)
	if err != nil {
		return obs, err
	}
	if c.Spec.Empty() {
		return obs, nil
	}

	var filledPeak, filledCost, derivedAchieved int
	var filledUUIDs []string
	for i := range obs.DeviceWindows {
		dw := &obs.DeviceWindows[i]
		entry, ok := c.Spec.lookup(dw.UUID, dw.Model)
		if !ok {
			continue
		}
		touched := false
		// Real series wins: only fill when the inner source left the field unknown.
		if entry.hasPeak() && !dw.PeakFLOPSKnown {
			dw.PeakFLOPS, dw.PeakFLOPSKnown = entry.PeakTFLOPS*1e12, true
			filledPeak++
			touched = true
			// On a vanilla dcgm-exporter box the achieved-FLOPs SERIES does not
			// exist either (it is the PromQL product tensor_active * peak, absent
			// without a peak series). When the inner source gave tensor-active but no
			// achieved-FLOPs, derive it from the now-known (spec) peak exactly as the
			// default PromQL would: achieved = tensor_active_ratio * peak * window =
			// TensorActiveSecs * peak. This is the SAME deterministic identity, not a
			// new fabrication — and only ever runs because the operator supplied the
			// peak. Without tensor-active there is nothing to derive (stays degraded).
			if dw.TensorActiveKnown && !dw.AchievedFLOPsKnown {
				dw.AchievedFLOPs = dw.TensorActiveSecs * dw.PeakFLOPS
				dw.AchievedFLOPsKnown = true
				derivedAchieved++
			}
		}
		if entry.hasCost() && !dw.CostKnown {
			dw.CostPerHour, dw.CostKnown = entry.CostPerHour, true
			filledCost++
			touched = true
		}
		if touched {
			filledUUIDs = append(filledUUIDs, dw.UUID)
		}
	}

	if filledPeak == 0 && filledCost == 0 {
		return obs, nil // spec present but everything was already real ⇒ no stamp
	}
	if obs.Provenance == nil {
		obs.Provenance = map[string]string{}
	}
	if filledPeak > 0 {
		obs.Provenance[provKeyPeakSource] = specSourceValue
	}
	if filledCost > 0 {
		obs.Provenance[provKeyCostSource] = specSourceValue
	}
	if derivedAchieved > 0 {
		// The achieved-FLOPs was derived from the real tensor-active series and the
		// spec peak (the default-PromQL identity). Stamp its origin so it is not
		// mistaken for a directly-metered achieved-FLOPs series.
		obs.Provenance[provKeyAchievedSource] = "derived:tensor-active*static-spec-peak"
	}
	sort.Strings(filledUUIDs)
	obs.Provenance[provKeySpecFilled] = strings.Join(filledUUIDs, ",")
	return obs, nil
}
