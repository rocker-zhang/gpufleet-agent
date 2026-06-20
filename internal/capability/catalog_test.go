package capability

import (
	"testing"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

func TestCatalog_EveryDescriptorIsWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, d := range Catalog() {
		if d.GetId() == "" {
			t.Error("descriptor with empty id")
		}
		if seen[d.GetId()] {
			t.Errorf("duplicate capability id %q", d.GetId())
		}
		seen[d.GetId()] = true
		if d.GetTier() == gpufleetv1.ConsentTier_CONSENT_TIER_UNSPECIFIED {
			t.Errorf("%s: tier unspecified", d.GetId())
		}
		// Every capability MUST carry a resource ceiling — the directive
		// validator clamps to it, so an absent cap would mean an unbounded run.
		if d.GetResourceBudget() == nil {
			t.Errorf("%s: no resource_budget cap (unbounded)", d.GetId())
		}
		if d.GetVersion() == "" {
			t.Errorf("%s: no version", d.GetId())
		}
	}
}

func TestFind(t *testing.T) {
	if Find("dcgm.fields") == nil {
		t.Error("known id not found")
	}
	if Find("nope") != nil {
		t.Error("unknown id should return nil, not a default")
	}
}

func TestDescribe(t *testing.T) {
	r := Describe()
	if r.GetName() == "" || r.GetVersion() == "" || r.GetContractVersion() == "" {
		t.Errorf("Describe missing identity fields: %+v", r)
	}
	if len(r.GetCatalog()) != len(Catalog()) {
		t.Errorf("Describe catalog size %d != %d", len(r.GetCatalog()), len(Catalog()))
	}
}

// Tier-1 capabilities must exist in the catalog (so the control plane knows they
// exist) but the catalog itself never marks them enabled — enablement is the
// host's runtime decision, enforced by the directive validator.
func TestCatalog_HasBothTiers(t *testing.T) {
	var t0, t1 int
	for _, d := range Catalog() {
		switch d.GetTier() {
		case gpufleetv1.ConsentTier_CONSENT_TIER_UNPRIVILEGED:
			t0++
		case gpufleetv1.ConsentTier_CONSENT_TIER_PRIVILEGED:
			t1++
		}
	}
	if t0 == 0 || t1 == 0 {
		t.Errorf("expected both tiers represented; got tier0=%d tier1=%d", t0, t1)
	}
}
