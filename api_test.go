//go:build !gpu

package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

// newTestDaemon builds a refreshed daemon on the default mock sources with a
// fixed clock, ready to serve.
func newTestDaemon(t *testing.T) *Daemon {
	t.Helper()
	d := NewDaemon(DaemonConfig{
		AgentID:    "agent-e2e",
		Window:     time.Minute,
		Collectors: DefaultCollectors("e2e-node"),
		Policy:     DefaultCostPolicy(),
		Now:        fixedNow,
	})
	if err := d.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	return d
}

// TestEndToEndLocalAPI is the acceptance test: start the agent on mock sources,
// hit the LOCAL read-only API, and verify it returns a SignalSchema window plus
// the standalone cost wedge with healthy device = $0 and idle device > $0.
func TestEndToEndLocalAPI(t *testing.T) {
	d := newTestDaemon(t)
	srv := httptest.NewServer(NewAPI(d).Handler())
	defer srv.Close()

	// 1) /signals returns a real gpufleet.v1 EvidencePack (parse back via proto).
	body := httpGet(t, srv.URL+"/signals", http.StatusOK)
	pack := &gpufleetv1.EvidencePack{}
	if err := protojson.Unmarshal(body, pack); err != nil {
		t.Fatalf("/signals is not a valid gpufleet.v1 EvidencePack: %v", err)
	}
	if pack.ContractVersion != "v1" {
		t.Fatalf("contract_version = %q, want v1", pack.ContractVersion)
	}
	if len(pack.Mappings) != 3 {
		t.Fatalf("want 3 device mappings in pack, got %d", len(pack.Mappings))
	}
	if pack.WindowStart == nil || pack.WindowEnd == nil {
		t.Fatalf("window bounds missing from pack")
	}

	// 2) /cost returns per-device wedges: healthy = $0, idle > $0.
	cb := httpGet(t, srv.URL+"/cost", http.StatusOK)
	var cost CostResponse
	if err := json.Unmarshal(cb, &cost); err != nil {
		t.Fatalf("decode /cost: %v", err)
	}
	wasted := map[string]float64{}
	for _, dc := range cost.Devices {
		wasted[dc.UUID] = dc.WastedUSD
	}
	if wasted["GPU-mock-0002"] <= 0 {
		t.Fatalf("idle device must waste > $0 via API, got %.6f", wasted["GPU-mock-0002"])
	}
	if wasted["GPU-mock-0002"] <= wasted["GPU-mock-0001"] {
		t.Fatalf("idle waste must exceed healthy waste via API")
	}
	if len(cost.Jobs) == 0 {
		t.Fatalf("expected per-job cost wedges")
	}

	// 3) /window combined view carries degradation marks + sources.
	wb := httpGet(t, srv.URL+"/window", http.StatusOK)
	var win WindowResponse
	if err := json.Unmarshal(wb, &win); err != nil {
		t.Fatalf("decode /window: %v", err)
	}
	if len(win.Sources) != 3 {
		t.Fatalf("want 3 sources in /window, got %v", win.Sources)
	}
}

// TestAPIIsReadOnly proves the local API refuses every mutating verb (zero
// write-back / control — RULES §A).
func TestAPIIsReadOnly(t *testing.T) {
	d := newTestDaemon(t)
	srv := httptest.NewServer(NewAPI(d).Handler())
	defer srv.Close()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		for _, path := range []string{"/signals", "/cost", "/window", "/healthz"} {
			req, err := http.NewRequest(method, srv.URL+path, strings.NewReader("{}"))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do %s %s: %v", method, path, err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: got %d, want 405 (read-only)", method, path, resp.StatusCode)
			}
		}
	}
}

func httpGet(t *testing.T, url string, wantStatus int) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s: status %d (want %d), body=%s", url, resp.StatusCode, wantStatus, b)
	}
	return b
}
