package directive

import (
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// allTiers / tier0Only model a host's consent configuration.
func allTiers(gpufleetv1.ConsentTier) bool { return true }
func tier0Only(t gpufleetv1.ConsentTier) bool {
	return t == gpufleetv1.ConsentTier_CONSENT_TIER_UNPRIVILEGED
}

func TestValidate_UnknownCapabilityRejected(t *testing.T) {
	_, _, err := Validate(&gpufleetv1.CollectDirective{CapabilityId: "evil.exfiltrate"}, allTiers)
	if err == nil {
		t.Fatal("unknown capability id must be rejected (allowlist)")
	}
}

func TestValidate_EmptyAndNilRejected(t *testing.T) {
	if _, _, err := Validate(nil, allTiers); err == nil {
		t.Error("nil directive must be rejected")
	}
	if _, _, err := Validate(&gpufleetv1.CollectDirective{}, allTiers); err == nil {
		t.Error("empty capability_id must be rejected")
	}
}

// THE CONSENT GATE: a Tier-1 capability must be refused when only Tier-0 is
// enabled, even though the id is a real catalog entry.
func TestValidate_Tier1RefusedWhenNotOptedIn(t *testing.T) {
	d := &gpufleetv1.CollectDirective{CapabilityId: "ebpf.nvlink.retrans"}
	if _, _, err := Validate(d, tier0Only); err == nil {
		t.Fatal("Tier-1 capability must be refused without opt-in")
	}
	// With privileged enabled it is allowed.
	if _, _, err := Validate(d, allTiers); err != nil {
		t.Fatalf("Tier-1 should pass when opted in: %v", err)
	}
}

func TestValidate_NilEnabledFuncRefuses(t *testing.T) {
	d := &gpufleetv1.CollectDirective{CapabilityId: "dcgm.fields"}
	if _, _, err := Validate(d, nil); err == nil {
		t.Fatal("a nil tier-policy must refuse, not default-allow")
	}
}

// THE BUDGET CEILING: a directive requesting MORE than the capability cap is
// clamped down to the cap, never executed above it.
func TestValidate_BudgetClampedToCap(t *testing.T) {
	// dcgm.fields cap: 10m / 200 mCPU / 6000 samples.
	d := &gpufleetv1.CollectDirective{
		CapabilityId: "dcgm.fields",
		Budget: &gpufleetv1.ResourceBudget{
			MaxDuration:      durationpb.New(2 * time.Hour), // way over cap
			MaxCpuMillicores: 100000,
			MaxSamples:       9_000_000,
		},
	}
	_, safe, err := Validate(d, allTiers)
	if err != nil {
		t.Fatal(err)
	}
	b := safe.GetBudget()
	if b.GetMaxDuration().AsDuration() != 10*time.Minute {
		t.Errorf("duration not clamped: %v", b.GetMaxDuration().AsDuration())
	}
	if b.GetMaxCpuMillicores() != 200 {
		t.Errorf("cpu not clamped: %d", b.GetMaxCpuMillicores())
	}
	if b.GetMaxSamples() != 6000 {
		t.Errorf("samples not clamped: %d", b.GetMaxSamples())
	}
}

// A request BELOW the cap is honored (the control plane may ask for less).
func TestValidate_BudgetBelowCapHonored(t *testing.T) {
	d := &gpufleetv1.CollectDirective{
		CapabilityId: "dcgm.fields",
		Budget:       &gpufleetv1.ResourceBudget{MaxCpuMillicores: 50, MaxSamples: 100},
	}
	_, safe, err := Validate(d, allTiers)
	if err != nil {
		t.Fatal(err)
	}
	if safe.GetBudget().GetMaxCpuMillicores() != 50 || safe.GetBudget().GetMaxSamples() != 100 {
		t.Errorf("under-cap request not honored: %+v", safe.GetBudget())
	}
}

// Zero ("collector default") yields the cap, not zero.
func TestValidate_ZeroBudgetUsesCap(t *testing.T) {
	d := &gpufleetv1.CollectDirective{
		CapabilityId: "dcgm.fields",
		Budget:       &gpufleetv1.ResourceBudget{}, // all zero
	}
	_, safe, err := Validate(d, allTiers)
	if err != nil {
		t.Fatal(err)
	}
	if safe.GetBudget().GetMaxCpuMillicores() != 200 {
		t.Errorf("zero cpu should use cap 200, got %d", safe.GetBudget().GetMaxCpuMillicores())
	}
}

// Validate must not mutate the caller's directive (it clamps a clone).
func TestValidate_DoesNotMutateInput(t *testing.T) {
	d := &gpufleetv1.CollectDirective{
		CapabilityId: "dcgm.fields",
		Budget:       &gpufleetv1.ResourceBudget{MaxCpuMillicores: 99999},
	}
	if _, _, err := Validate(d, allTiers); err != nil {
		t.Fatal(err)
	}
	if d.GetBudget().GetMaxCpuMillicores() != 99999 {
		t.Error("input directive was mutated")
	}
}
