// Package capability is the agent's FIXED capability catalog and the Describe
// handshake (D-0011). The agent advertises exactly this closed set of collectors
// to the control plane; the control plane may then send a declarative
// CollectDirective that NAMES one of these ids — never code. The catalog is the
// allowlist the directive validator enforces against.
//
// Tiers (gpufleet.v1.ConsentTier):
//   - UNPRIVILEGED (Tier-0): DCGM / Prometheus / dmesg. On by default.
//   - PRIVILEGED   (Tier-1): read-only eBPF / perf observers. Requires explicit
//     customer opt-in; never on by default.
//
// Every descriptor carries a ResourceBudget that is the HARD CEILING for any
// directive selecting it — the validator clamps a directive's requested budget
// down to these caps and never above.
package capability

import (
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// catalogVersion is the catalog's own revision, bumped when descriptors change.
const catalogVersion = "v0.2.0"

// builtin is the agent's fixed catalog. IDs are stable wire identifiers the
// control plane addresses; they never change meaning across versions.
var builtin = []*gpufleetv1.CapabilityDescriptor{
	{
		Id:          "dcgm.fields",
		Tier:        gpufleetv1.ConsentTier_CONSENT_TIER_UNPRIVILEGED,
		Source:      gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
		Description: "DCGM field-group sampling (utilization, ECC, NVLink/PCIe counters).",
		Version:     "1.0.0",
		ResourceBudget: &gpufleetv1.ResourceBudget{
			MaxDuration:      durationpb.New(10 * time.Minute),
			MaxCpuMillicores: 200,
			MaxSamples:       6000,
		},
	},
	{
		Id:          "prometheus.query",
		Tier:        gpufleetv1.ConsentTier_CONSENT_TIER_UNPRIVILEGED,
		Source:      gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS,
		Description: "Range/instant queries against a configured Prometheus exporter.",
		Version:     "1.0.0",
		ResourceBudget: &gpufleetv1.ResourceBudget{
			MaxDuration:      durationpb.New(5 * time.Minute),
			MaxCpuMillicores: 100,
			MaxSamples:       4000,
		},
	},
	{
		Id:          "dmesg.xid",
		Tier:        gpufleetv1.ConsentTier_CONSENT_TIER_UNPRIVILEGED,
		Source:      gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
		Description: "Kernel ring-buffer XID/NVRM event scrape (read-only).",
		Version:     "1.0.0",
		ResourceBudget: &gpufleetv1.ResourceBudget{
			MaxDuration:      durationpb.New(10 * time.Minute),
			MaxCpuMillicores: 50,
			MaxSamples:       2000,
		},
	},
	{
		// Tier-1: declared so the control plane knows it EXISTS, but never
		// executes unless the customer opted into PRIVILEGED. The kernel attach
		// itself is a separate collector (not in this card).
		Id:          "ebpf.nvlink.retrans",
		Tier:        gpufleetv1.ConsentTier_CONSENT_TIER_PRIVILEGED,
		Source:      gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
		Description: "Read-only eBPF observer for NVLink retransmit events (CAP_BPF/CAP_PERFMON).",
		Version:     "0.1.0",
		ResourceBudget: &gpufleetv1.ResourceBudget{
			MaxDuration:      durationpb.New(2 * time.Minute),
			MaxCpuMillicores: 150,
			MaxMapEntries:    4096,
			MaxSamples:       2000,
		},
	},
}

// Catalog returns a copy of the fixed descriptor list (callers must not mutate
// the shared descriptors).
func Catalog() []*gpufleetv1.CapabilityDescriptor {
	out := make([]*gpufleetv1.CapabilityDescriptor, len(builtin))
	copy(out, builtin)
	return out
}

// Find returns the descriptor with the given id, or nil if it is not in the
// catalog (an unknown id is a rejected directive, never a silent default).
func Find(id string) *gpufleetv1.CapabilityDescriptor {
	for _, d := range builtin {
		if d.GetId() == id {
			return d
		}
	}
	return nil
}

// Describe builds the DescribeResponse the agent returns to the control plane:
// the full catalog plus its version. The control plane learns what CAN be
// enabled; tier enforcement happens when a directive arrives (the agent never
// runs a Tier-1 capability the customer did not opt into).
func Describe() *gpufleetv1.DescribeResponse {
	return &gpufleetv1.DescribeResponse{
		Name:            "gpufleet-agent",
		Version:         catalogVersion,
		ContractVersion: "v0.2.0",
		Catalog:         Catalog(),
	}
}
