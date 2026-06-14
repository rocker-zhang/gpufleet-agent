package agent

import (
	"regexp"
	"sort"
	"strings"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file is the BUILT-IN GPU peak-FLOPS table (TASK-0044). On a vanilla
// dcgm-exporter box the tensor-peak field (DCGM_FI_DEV_TENSOR_PEAK_FLOPS) does
// NOT exist, so MFU silently degrades to 0 and the operator has to hand-supply
// --peak-tflops. But the peak FLOPs of a known datacenter GPU is a published
// per-(model x precision) DATASHEET CONSTANT, and the agent already sees the
// DCGM `modelName` label (e.g. "NVIDIA A10"). So it can AUTO-RESOLVE the peak
// from the model name — zero-config real MFU.
//
// PRECISION BASIS (honesty, RULES §B): every entry is the NVIDIA datasheet
// **FP16/BF16 DENSE tensor** peak (NOT the 2x sparse number, NOT FP8/FP4, NOT
// FP32/FP64). This single, documented basis matches how the cost wedge computes
// MFU (achieved tensor FLOPs / peak). The basis is stamped into provenance
// (peak.source=builtin-table:<model>@fp16-dense) so a reviewer can always audit
// the origin AND the precision assumption behind any auto-filled peak.
//
// degrade-not-fabricate (RULES §B) is preserved with a STRICT precedence:
//
//	real telemetry peak (DCGM/Prom series) > operator-explicit
//	--peak-tflops/--device-spec-file > built-in table (by modelName) > DEGRADE.
//
// The table is the LAST resort BEFORE degrade: it only fills a peak that neither
// a real series nor an explicit operator spec supplied. An UNKNOWN model is NOT
// guessed — it DEGRADES (the field stays unknown, marked). This is build-
// AGNOSTIC: it touches only the scalar DeviceWindow.PeakFLOPS, no NVML, so it
// compiles in the default !gpu build.
//
// The table is FLOPs ONLY. It deliberately carries NO $/hr — a price is an
// operator input (a datasheet cannot know what a customer pays), so cost stays
// operator-supplied (spec/--cost-usd-per-hour) and degrades when absent.

// gpuPeakEntry is one built-in datasheet peak: the canonical model key and its
// FP16/BF16 dense tensor peak in TFLOP/s. PeakTFLOPS is in TFLOP/s for
// readability (converted to FLOP/s on fill to match DeviceWindow.PeakFLOPS).
type gpuPeakEntry struct {
	// Canonical is the canonical model name stamped into provenance.
	Canonical string
	// PeakTFLOPS is the FP16/BF16 DENSE tensor peak in TFLOP/s (NVIDIA datasheet).
	PeakTFLOPS float64
}

// builtinGPUPeaks maps a canonical GPU model to its FP16/BF16 DENSE tensor peak
// (TFLOP/s), per the public NVIDIA datasheet for each board. Numbers are the
// DENSE figure (the sparse/structured-sparsity number is 2x and is NOT used).
// Sources are the NVIDIA product datasheets / architecture whitepapers; the
// figure cited is the "BFLOAT16/FP16 Tensor Core" dense TFLOPS.
//
// Keys are already canonical (lower-case, no "nvidia " prefix); matchModelPeak
// normalizes an incoming modelName to this space and applies tolerant fuzzy
// matching for SKU suffixes (e.g. "A100-SXM4-80GB" -> "a100-80gb").
var builtinGPUPeaks = map[string]gpuPeakEntry{
	// A10 — Ampere, GA102. Datasheet: 125 TFLOPS BF16/FP16 dense Tensor.
	"a10": {Canonical: "A10", PeakTFLOPS: 125},
	// A100 40GB — Ampere, GA100. Datasheet: 312 TFLOPS BF16/FP16 dense Tensor.
	"a100-40gb": {Canonical: "A100-40GB", PeakTFLOPS: 312},
	// A100 80GB — Ampere, GA100. Same compute die as 40GB: 312 TFLOPS BF16/FP16 dense.
	"a100-80gb": {Canonical: "A100-80GB", PeakTFLOPS: 312},
	// H100 SXM5 — Hopper, GH100. Datasheet: 989.4 TFLOPS BF16/FP16 dense Tensor (~990).
	"h100-sxm": {Canonical: "H100-SXM", PeakTFLOPS: 989.4},
	// H100 PCIe — Hopper, GH100, lower clocks/TDP. Datasheet: 756 TFLOPS BF16/FP16 dense.
	"h100-pcie": {Canonical: "H100-PCIe", PeakTFLOPS: 756},
	// H100 NVL — Hopper, GH100, dual-GPU NVL board; per-GPU clocks higher than PCIe.
	// NVIDIA H100 NVL datasheet: 835.5 TFLOPS BF16/FP16 dense Tensor PER GPU (~835).
	"h100-nvl": {Canonical: "H100-NVL", PeakTFLOPS: 835},
	// H200 SXM — Hopper, GH100 (memory-upgraded H100). Same compute: 989.4 TFLOPS BF16/FP16 dense.
	"h200": {Canonical: "H200", PeakTFLOPS: 989.4},
	// GH200 — Grace-Hopper superchip. Shares the H100 GH100 die: 989.4 TFLOPS
	// BF16/FP16 dense Tensor (same as H100-SXM). Source: NVIDIA GH200 datasheet.
	"gh200": {Canonical: "GH200", PeakTFLOPS: 989.4},
	// L4 — Ada Lovelace, AD104. Datasheet: 121 TFLOPS BF16/FP16 dense Tensor.
	"l4": {Canonical: "L4", PeakTFLOPS: 121},
	// L40 — Ada Lovelace, AD102. Datasheet: 181.05 TFLOPS BF16/FP16 dense Tensor (~181).
	"l40": {Canonical: "L40", PeakTFLOPS: 181},
	// L40S — Ada Lovelace, AD102, higher clocks. Datasheet: 362.05 TFLOPS BF16/FP16 dense (~362).
	"l40s": {Canonical: "L40S", PeakTFLOPS: 362},
	// V100 SXM2/PCIe — Volta, GV100. Datasheet: 125 TFLOPS FP16 Tensor (no BF16; FP16 only).
	"v100": {Canonical: "V100", PeakTFLOPS: 125},
	// T4 — Turing, TU104. Datasheet: 65 TFLOPS FP16 Tensor (mixed-precision; no BF16 dense path).
	"t4": {Canonical: "T4", PeakTFLOPS: 65},
	// RTX A6000 — Ampere, GA102 (workstation, common in labs). 154.8 TFLOPS BF16/FP16 dense (~155).
	"a6000": {Canonical: "A6000", PeakTFLOPS: 155},
	// GB10 — DELIBERATELY ABSENT (RULES §B degrade-not-fabricate). The widely-cited
	// "1 petaFLOP" GB10 / DGX Spark headline is the FP4-WITH-SPARSITY marketing
	// figure — NOT FP16/BF16 dense, so it would BREAK this table's basis. NVIDIA
	// publishes no confident FP16/BF16-dense per-GPU datasheet number for GB10, so
	// rather than ship an inflated/derived value we DROP the entry: a GB10 box
	// matches NOTHING → peak degrades → the operator supplies --peak-tflops. Add a
	// real FP16/BF16-dense datasheet figure here only once one is published.
	// B200 — Blackwell, single GPU. Datasheet: 2250 TFLOPS BF16/FP16 dense Tensor (~2.25 PFLOPS).
	"b200": {Canonical: "B200", PeakTFLOPS: 2250},
}

// sku100CapRe extracts the memory capacity (GB) embedded in an A100 SKU string,
// e.g. "a100-sxm4-80gb" -> "80", "a100-pcie-40gb" -> "40", "a100 80gb" -> "80".
// Group 1 is the LAST number immediately before "gb", so intermediate SKU digits
// like the "4" in "sxm4" are not mistaken for the capacity.
var sku100CapRe = regexp.MustCompile(`(\d+)\s*gb`)

// gpuPeakProvenanceValue is the provenance VALUE stamped for a table-filled peak;
// the canonical model is appended (builtin-table:<model>@fp16-dense), recording
// BOTH the table origin AND the FP16/BF16 dense precision basis (RULES §B).
const gpuPeakProvenancePrefix = "builtin-table:"
const gpuPeakProvenanceBasis = "@fp16-dense"

// builtinTablePeakSource builds the provenance value for a model the table filled.
func builtinTablePeakSource(canonical string) string {
	return gpuPeakProvenancePrefix + canonical + gpuPeakProvenanceBasis
}

// matchModelPeak resolves a DCGM modelName to its built-in FP16/BF16 dense peak.
// Matching is case-insensitive, tolerant of a leading "NVIDIA " vendor prefix and
// of SKU suffixes (e.g. "NVIDIA A100-SXM4-80GB" -> A100-80GB, "Tesla T4" -> T4,
// "NVIDIA H100 80GB HBM3" -> H100-SXM). An unknown model returns ok=false so the
// caller DEGRADES — it never guesses (RULES §B). Returns the canonical model and
// peak in TFLOP/s.
func matchModelPeak(model string) (canonical string, peakTFLOPS float64, ok bool) {
	n := normModel(model) // lower-cased, trimmed, "nvidia " prefix dropped (device_spec.go)
	// Strip other vendor/line prefixes DCGM may emit: "tesla " (e.g. "Tesla T4"),
	// "rtx " (e.g. "RTX A6000"). Done in a small loop so a combined "nvidia rtx …"
	// (the "nvidia " already gone via normModel) collapses to the bare model token.
	for _, p := range []string{"tesla ", "rtx ", "quadro "} {
		n = strings.TrimSpace(strings.TrimPrefix(n, p))
	}
	if n == "" {
		return "", 0, false
	}

	// 1) Exact canonical hit (covers "a10", "l40s", "v100", "gb10", ...).
	if e, hit := builtinGPUPeaks[n]; hit {
		return e.Canonical, e.PeakTFLOPS, true
	}

	// 2) A100 with an embedded capacity SKU: pick the 40GB/80GB variant. Anchored to
	// an "a100" token start so "a1000"/other models can't accidentally match.
	if n == "a100" || strings.HasPrefix(n, "a100-") || strings.HasPrefix(n, "a100 ") {
		if m := sku100CapRe.FindStringSubmatch(n); m != nil {
			switch m[1] {
			case "80":
				e := builtinGPUPeaks["a100-80gb"]
				return e.Canonical, e.PeakTFLOPS, true
			case "40":
				e := builtinGPUPeaks["a100-40gb"]
				return e.Canonical, e.PeakTFLOPS, true
			}
		}
		// Bare "a100" or an unknown capacity: both variants share the same compute
		// die (312 dense), so that figure is safe (not a guess about which SKU).
		e := builtinGPUPeaks["a100-40gb"]
		return e.Canonical, e.PeakTFLOPS, true
	}

	// 3) H100 variant disambiguation by form factor in the SKU string. NVL is its
	// own per-GPU dense figure (~835), distinct from the PCIe board (756); check it
	// FIRST so "H100 NVL" doesn't fall into the PCIe bucket.
	if strings.HasPrefix(n, "h100") {
		if strings.Contains(n, "nvl") {
			e := builtinGPUPeaks["h100-nvl"]
			return e.Canonical, e.PeakTFLOPS, true
		}
		if strings.Contains(n, "pcie") || strings.Contains(n, "pci-e") {
			e := builtinGPUPeaks["h100-pcie"]
			return e.Canonical, e.PeakTFLOPS, true
		}
		// Default H100 form factor is SXM (the datacenter default).
		e := builtinGPUPeaks["h100-sxm"]
		return e.Canonical, e.PeakTFLOPS, true
	}

	// 4) Generic prefix/whole-token fuzzy match for the remaining models. We match
	// the LONGEST canonical key that the normalized name begins with on a token
	// boundary, so "l40s …" prefers "l40s" over "l40", and "a6000 …" hits "a6000".
	// We deliberately do NOT do substring-anywhere matching (it would mis-hit,
	// e.g. "l4" inside "l40"); only a leading-token match counts.
	type cand struct {
		key string
		e   gpuPeakEntry
	}
	var best cand
	for k, e := range builtinGPUPeaks {
		if n == k || strings.HasPrefix(n, k+" ") || strings.HasPrefix(n, k+"-") {
			if len(k) > len(best.key) {
				best = cand{key: k, e: e}
			}
		}
	}
	if best.key != "" {
		return best.e.Canonical, best.e.PeakTFLOPS, true
	}

	return "", 0, false
}

// Provenance keys stamped when a built-in-table value fills a peak gap, so the
// origin (and FP16/BF16 dense precision basis) of an auto-resolved peak is always
// auditable (RULES §B): a reviewer can tell a table-sourced peak from a real
// metered one or an operator-explicit spec one.
const (
	provKeyBuiltinPeakSource = "builtin.peak.source"
	provKeyBuiltinFilled     = "builtin.filled_devices"
)

// PeakTableCollector wraps an inner metrics collector and, as the LAST resort
// before degrade, FILLS a device's missing PeakFLOPS from the built-in datasheet
// table keyed by DCGM modelName. It runs AFTER the operator's SpecFillCollector,
// so the strict precedence holds automatically:
//
//	real series (inner kept it known) > operator spec (SpecFillCollector filled
//	it) > built-in table (this fills only what is STILL unknown) > degrade.
//
// It is FLOPs-ONLY: it never touches cost. An unknown model is left unknown
// (degrade, never fabricated). Every peak it fills is stamped
// peak.source=builtin-table:<model>@fp16-dense. It is read-only and build-
// agnostic (scalar PeakFLOPS only, no NVML). Disabled by Enabled=false ⇒
// transparent passthrough.
type PeakTableCollector struct {
	// Inner is the wrapped collector (typically the metrics chain already wrapped
	// in SpecFillCollector, so the operator spec has had first refusal).
	Inner Collector
	// Enabled gates the table. False ⇒ transparent passthrough (no fill, no stamp),
	// so an operator can opt out and keep pure degrade-not-fabricate.
	Enabled bool
}

// Source delegates to the inner collector: the table adds no new SignalSource, it
// only completes the peak field the inner source attributed to a device.
func (c PeakTableCollector) Source() gpufleetv1.SignalSource { return c.Inner.Source() }

// Collect runs the inner collector then fills any STILL-unknown per-device peak
// from the built-in table (by modelName), stamping provenance. A peak the inner
// chain already supplied (real series OR operator spec) is left untouched —
// real/operator-explicit always win. When the now-known table peak unlocks the
// PromQL achieved-FLOPs identity (real tensor-active * peak), it derives achieved
// exactly as the default PromQL would (same deterministic identity, not a new
// fabrication — and only because a known model supplied the peak). An unknown
// model fills nothing and the peak keeps degrading. Cost is never touched.
func (c PeakTableCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	obs, err := c.Inner.Collect(now, window)
	if err != nil {
		return obs, err
	}
	if !c.Enabled {
		return obs, nil
	}

	var filled, derivedAchieved int
	filledByModel := map[string]bool{}
	var filledUUIDs []string
	for i := range obs.DeviceWindows {
		dw := &obs.DeviceWindows[i]
		// Real series or operator spec already supplied the peak ⇒ they win; skip.
		if dw.PeakFLOPSKnown {
			continue
		}
		if dw.Model == "" {
			continue // no model to look up ⇒ degrade (cannot guess)
		}
		canonical, peakTFLOPS, ok := matchModelPeak(dw.Model)
		if !ok {
			continue // unknown model ⇒ DEGRADE, never fabricate (RULES §B)
		}
		dw.PeakFLOPS, dw.PeakFLOPSKnown = peakTFLOPS*1e12, true
		filled++
		filledByModel[canonical] = true
		filledUUIDs = append(filledUUIDs, dw.UUID)
		// Derive achieved-FLOPs from the REAL tensor-active series and the now-known
		// table peak (the default-PromQL identity), exactly as SpecFillCollector does
		// for a spec peak. Without tensor-active there is nothing to derive (degrade).
		if dw.TensorActiveKnown && !dw.AchievedFLOPsKnown {
			dw.AchievedFLOPs = dw.TensorActiveSecs * dw.PeakFLOPS
			dw.AchievedFLOPsKnown = true
			derivedAchieved++
		}
	}

	if filled == 0 {
		return obs, nil // nothing matched ⇒ no stamp (peaks stay degraded)
	}
	if obs.Provenance == nil {
		obs.Provenance = map[string]string{}
	}
	// Stamp the per-model table source(s). With a homogeneous box this is a single
	// builtin-table:<model>@fp16-dense; a heterogeneous box lists each model's
	// source comma-joined, deterministically ordered.
	models := make([]string, 0, len(filledByModel))
	for m := range filledByModel {
		models = append(models, m)
	}
	sort.Strings(models)
	srcs := make([]string, 0, len(models))
	for _, m := range models {
		srcs = append(srcs, builtinTablePeakSource(m))
	}
	obs.Provenance[provKeyBuiltinPeakSource] = strings.Join(srcs, ",")
	if derivedAchieved > 0 {
		obs.Provenance[provKeyAchievedSource] = "derived:tensor-active*builtin-table-peak"
	}
	sort.Strings(filledUUIDs)
	obs.Provenance[provKeyBuiltinFilled] = strings.Join(filledUUIDs, ",")
	return obs, nil
}
