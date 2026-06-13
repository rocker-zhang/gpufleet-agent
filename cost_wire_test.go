//go:build !gpu

package agent

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// TestCostWireGoldenShape pins the /cost JSON wire contract documented in
// CLAUDE.md §/cost. It decodes the committed golden fixture
// (testdata/cost_golden.json) into the agent's own CostResponse DTO and asserts
// the field shape — most importantly that usd_per_hour is present and carries
// the per-HOUR burn rate (distinct from per-window wasted_usd). The SAME fixture
// is committed under cli/testdata so the cli's hand-copied DTO is checked against
// byte-identical bytes; this test is the producer-side half of that anti-drift
// pair.
func TestCostWireGoldenShape(t *testing.T) {
	b, err := os.ReadFile("testdata/cost_golden.json")
	if err != nil {
		t.Fatalf("read golden fixture: %v", err)
	}

	// Decode disallowing unknown fields: any field the golden carries that the
	// DTO lacks (or vice-versa drift) is caught here.
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var cost CostResponse
	if err := dec.Decode(&cost); err != nil {
		t.Fatalf("golden /cost does not match agent CostResponse DTO: %v", err)
	}

	byUUID := map[string]DeviceCost{}
	for _, d := range cost.Devices {
		byUUID[d.UUID] = d
	}

	// Priced idle device: usd_per_hour is a RATE and, for this sub-hour window,
	// strictly exceeds the per-window wasted_usd.
	idle, ok := byUUID["GPU-mock-0002"]
	if !ok {
		t.Fatalf("golden missing GPU-mock-0002")
	}
	if !idle.Priced {
		t.Fatalf("GPU-mock-0002 must be priced")
	}
	if idle.UsdPerHour <= 0 {
		t.Errorf("priced idle device must report usd_per_hour > 0, got %v", idle.UsdPerHour)
	}
	if idle.UsdPerHour <= idle.WastedUSD {
		t.Errorf("idle per-hour rate (%v) must exceed per-window waste (%v)", idle.UsdPerHour, idle.WastedUSD)
	}

	// Healthy-ish device: lower per-hour than the idle device (rate scales with idle).
	healthy, ok := byUUID["GPU-mock-0001"]
	if !ok {
		t.Fatalf("golden missing GPU-mock-0001")
	}
	if healthy.UsdPerHour >= idle.UsdPerHour {
		t.Errorf("less-idle device per-hour (%v) should be below idle device per-hour (%v)", healthy.UsdPerHour, idle.UsdPerHour)
	}

	// Unpriced device: priced==false ⇒ usd_per_hour (and wasted_usd) are zero and
	// carry no meaning. The consumer must degrade-mark, not render $0.
	unpriced, ok := byUUID["GPU-mock-unpriced"]
	if !ok {
		t.Fatalf("golden missing GPU-mock-unpriced")
	}
	if unpriced.Priced {
		t.Errorf("GPU-mock-unpriced must be priced=false")
	}
	if unpriced.UsdPerHour != 0 || unpriced.WastedUSD != 0 {
		t.Errorf("unpriced device must zero its $ fields, got per_hour=%v wasted=%v", unpriced.UsdPerHour, unpriced.WastedUSD)
	}

	// Jobs carry the aggregate usd_per_hour too.
	if len(cost.Jobs) == 0 {
		t.Fatalf("golden missing jobs")
	}
	for _, j := range cost.Jobs {
		if j.Priced && j.UsdPerHour <= 0 {
			t.Errorf("priced job %s must report usd_per_hour > 0, got %v", j.JobID, j.UsdPerHour)
		}
	}
}
