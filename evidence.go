package agent

import "sort"

// ----------------------------------------------------------------------------
// Legacy single-reader evidence path (M1 scaffold).
//
// This pre-dates the multi-source Collector spine (collector.go) and is kept
// for the simple `nvml_gpu.go` gpu-tagged reader and back-compat. New work
// flows through Collector -> Normalize -> SignalWindow. Both paths are
// read-only and off-critical-path.
// ----------------------------------------------------------------------------

// DeviceMetrics is one normalized device measurement read from a source.
// All fields are already in normalized units; the agent does not interpret
// vendor-specific semantics here.
type DeviceMetrics struct {
	UUID             string
	Node             string
	Model            string
	WindowSeconds    float64
	AchievedFLOPs    float64
	TensorActiveSecs float64
	ECCDoubleBitErrs uint64 // delta over the window (corroborating RCA signal)
	XIDs             []int  // NVRM Xid codes observed in dmesg during the window
}

// Evidence is the normalized signal window the agent emits. It is the ONLY
// thing sent to the closed control plane (as a structured evidence pack) —
// never prompts, playbooks, or heuristics. The control plane returns a Verdict.
type Evidence struct {
	Source  string          // which reader produced this (e.g. "mock", "dcgm")
	Devices []DeviceMetrics // sorted by UUID for deterministic output
}

// Reader is the read-only metrics source interface. Implementations must not
// mutate or control the GPU in any way.
type Reader interface {
	// Read returns a normalized snapshot. It must be side-effect-free w.r.t.
	// the GPU and the running job.
	Read() (Evidence, error)
}

// Collect reads from r and returns evidence with devices sorted deterministically.
func Collect(r Reader) (Evidence, error) {
	ev, err := r.Read()
	if err != nil {
		return Evidence{}, err
	}
	sort.Slice(ev.Devices, func(i, j int) bool {
		return ev.Devices[i].UUID < ev.Devices[j].UUID
	})
	return ev, nil
}
