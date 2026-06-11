package agent

import (
	"sort"

	semantics "github.com/rocker-zhang/gpufleet-semantics"
)

// CostReport is the standalone cost-wedge view computed over a SignalWindow. It
// is the "money before RCA" path (RULES-friendly): cost/efficiency is derived
// purely from measured MFU/tensor-active + the $/hour rate, with NO fault, RCA,
// or gate dependency. A healthy device yields WastedUSD == $0; an idle device
// yields WastedUSD > $0. Devices whose MFU inputs were degraded are omitted
// (they appear in SignalWindow.Degraded, not fabricated here).
type CostReport struct {
	// Devices are per-device standalone cost wedges, sorted by UUID.
	Devices []semantics.CostWedge
	// Jobs are per-job aggregated cost wedges, sorted by job ID.
	Jobs []semantics.JobCostImpact
}

// CostWedge computes the standalone per-device and per-job cost wedges for a
// window via semantics.DeviceCostWedge / semantics.JobCostWedge. It calls ONLY
// the semantics standalone path — zero fault/gate dependency (RULES §A/§B).
func (w *SignalWindow) CostWedge(policy semantics.CostPolicy) (CostReport, error) {
	var rep CostReport

	// Per-device wedges, and an index of computed efficiencies for the job pass.
	effByUUID := map[string]semantics.DeviceEfficiency{}
	uuids := make([]string, 0, len(w.Samples))
	for u := range w.Samples {
		uuids = append(uuids, u)
	}
	sort.Strings(uuids)

	for _, u := range uuids {
		wedge, err := semantics.DeviceCostWedge(w.Samples[u], w.Specs[u], policy)
		if err != nil {
			return CostReport{}, err
		}
		rep.Devices = append(rep.Devices, wedge)
		effByUUID[u] = wedge.Device
	}

	// Per-job aggregation over the resolved device->job grouping. Only devices
	// with a computed efficiency (non-degraded MFU) participate.
	for _, jd := range w.Jobs {
		var effs []semantics.DeviceEfficiency
		specs := map[string]semantics.DeviceSpec{}
		for _, d := range jd.Devices {
			eff, ok := effByUUID[d.UUID]
			if !ok {
				continue // degraded device — not fabricated into the job total
			}
			effs = append(effs, eff)
			specs[d.UUID] = w.Specs[d.UUID]
		}
		if len(effs) == 0 {
			continue
		}
		rep.Jobs = append(rep.Jobs, semantics.JobCostWedge(jd.Job, effs, specs, policy))
	}
	return rep, nil
}
