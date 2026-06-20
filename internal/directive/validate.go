// Package directive enforces D-0011 on an incoming CollectDirective: the
// declarative instruction the control plane sends to run a NAMED capability. The
// agent is the final authority on what runs on its host — it never trusts the
// control plane blindly. A directive may only:
//   - name a capability id in the FIXED catalog (allowlist; unknown id rejected),
//   - whose ConsentTier the customer has ENABLED (Tier-1 off by default),
//   - within the capability's advertised ResourceBudget (requests are CLAMPED
//     down to the caps, never allowed above).
//
// A CollectDirective carries NO code, bytecode, threshold, or heuristic — the
// proto has no such field (D-0011). This package enforces the remaining runtime
// guarantees so a compromised/rogue control plane cannot widen the agent's blast
// radius beyond what the host operator opted into.
package directive

import (
	"fmt"

	"github.com/rocker-zhang/gpufleet-agent/internal/capability"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TierEnabled reports whether a consent tier is enabled on this host.
type TierEnabled func(gpufleetv1.ConsentTier) bool

// Validate checks a directive against the catalog and the host's enabled tiers.
// On success it returns the matched descriptor and a directive whose budget has
// been CLAMPED to the descriptor's caps (safe to execute). On failure it returns
// a non-nil error and the directive must NOT run.
func Validate(d *gpufleetv1.CollectDirective, enabled TierEnabled) (*gpufleetv1.CapabilityDescriptor, *gpufleetv1.CollectDirective, error) {
	if d == nil {
		return nil, nil, fmt.Errorf("directive: nil directive")
	}
	id := d.GetCapabilityId()
	if id == "" {
		return nil, nil, fmt.Errorf("directive: empty capability_id")
	}

	// Allowlist: the id MUST be a known catalog capability.
	desc := capability.Find(id)
	if desc == nil {
		return nil, nil, fmt.Errorf("directive: unknown capability_id %q (not in catalog)", id)
	}

	// Consent gate: the capability's tier must be enabled on this host. A Tier-1
	// capability the customer never opted into is refused even though it exists.
	if enabled == nil || !enabled(desc.GetTier()) {
		return nil, nil, fmt.Errorf("directive: capability %q requires tier %s, not enabled on this host",
			id, desc.GetTier())
	}

	// Budget: clamp the requested budget DOWN to the descriptor's caps. We never
	// reject for asking too much — we run the safe, clamped budget — so a
	// directive can never cause the agent to exceed the advertised ceiling.
	safe := proto.Clone(d).(*gpufleetv1.CollectDirective)
	safe.Budget = clampBudget(d.GetBudget(), desc.GetResourceBudget())
	return desc, safe, nil
}

// clampBudget returns a budget that is the per-field minimum of requested and the
// capability cap. A requested 0 ("collector default") yields the cap; a request
// above the cap is clamped to the cap. The cap is the hard ceiling.
func clampBudget(req, cap *gpufleetv1.ResourceBudget) *gpufleetv1.ResourceBudget {
	if cap == nil {
		// Defensive: a catalog descriptor MUST declare a budget (the catalog
		// well-formedness test enforces this). If one ever doesn't, the validator
		// is the security boundary and must NOT trust that — refuse to pass the
		// request through unclamped. Return an empty budget so the collector falls
		// back to its own safe defaults rather than honoring an attacker's budget.
		return &gpufleetv1.ResourceBudget{}
	}
	out := &gpufleetv1.ResourceBudget{
		MaxDuration:      cap.GetMaxDuration(),
		MaxCpuMillicores: cap.GetMaxCpuMillicores(),
		MaxMapEntries:    cap.GetMaxMapEntries(),
		MaxSamples:       cap.GetMaxSamples(),
	}
	if req != nil {
		out.MaxDuration = clampDuration(req.GetMaxDuration(), cap.GetMaxDuration())
		out.MaxCpuMillicores = clampU32(req.GetMaxCpuMillicores(), cap.GetMaxCpuMillicores())
		out.MaxMapEntries = clampU32(req.GetMaxMapEntries(), cap.GetMaxMapEntries())
		out.MaxSamples = clampU32(req.GetMaxSamples(), cap.GetMaxSamples())
	}
	return out
}

// clampDuration returns the lower positive of req and cap, mirroring clampU32. A
// nil cap means "no duration ceiling" → honor the request. A nil/non-positive or
// over-cap request falls back to the cap.
func clampDuration(req, cap *durationpb.Duration) *durationpb.Duration {
	if cap == nil {
		return req // no ceiling declared → honor the request as-is
	}
	if req == nil || req.AsDuration() <= 0 || req.AsDuration() > cap.AsDuration() {
		return cap
	}
	return req
}

// clampU32 returns the lower positive of req and cap. A req of 0 ("use the
// collector default") yields cap; a req above cap is clamped to cap.
func clampU32(req, cap uint32) uint32 {
	if req == 0 || req > cap {
		return cap
	}
	return req
}
