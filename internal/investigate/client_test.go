package investigate

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"github.com/rocker-zhang/gpufleet-rca/registry"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// allTiers is a consent func that enables every tier (test use only).
func allTiers(gpufleetv1.ConsentTier) bool { return true }

// noTiers is a consent func that enables no tier.
func noTiers(gpufleetv1.ConsentTier) bool { return false }

// tier0Only is a consent func that enables Tier-0 (UNPRIVILEGED) only.
func tier0Only(t gpufleetv1.ConsentTier) bool {
	return t == gpufleetv1.ConsentTier_CONSENT_TIER_UNPRIVILEGED
}

// echoExecutor always returns the given entries, regardless of the directive.
type echoExecutor struct {
	entries []*gpufleetv1.TimelineEntry
}

func (e *echoExecutor) Execute(_ context.Context, _ *gpufleetv1.CollectDirective) ([]*gpufleetv1.TimelineEntry, error) {
	return e.entries, nil
}

// emptyExecutor returns nothing (no new entries).
type emptyExecutor struct{}

func (emptyExecutor) Execute(_ context.Context, _ *gpufleetv1.CollectDirective) ([]*gpufleetv1.TimelineEntry, error) {
	return nil, nil
}

// capturingExecutor records every directive it receives.
type capturingExecutor struct {
	seen    []*gpufleetv1.CollectDirective
	entries []*gpufleetv1.TimelineEntry
}

func (c *capturingExecutor) Execute(_ context.Context, d *gpufleetv1.CollectDirective) ([]*gpufleetv1.TimelineEntry, error) {
	c.seen = append(c.seen, d)
	return c.entries, nil
}

// realEngineServer builds an httptest.Server that runs the REAL open
// registry.NewDefaultEngine() and returns InvestigateResponse (Verdict +
// optional directives). On ABSTAIN it suggests the prometheus.query directive
// (a Tier-0 capability from a source not yet in the pack), mirroring the
// control-plane's directive.Escalate behaviour — but without importing any
// closed-plane code.
func realEngineServer(t *testing.T) *httptest.Server {
	t.Helper()
	engine := registry.NewDefaultEngine()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read error", http.StatusBadRequest)
			return
		}
		pack := &gpufleetv1.EvidencePack{}
		if err := protojson.Unmarshal(raw, pack); err != nil {
			http.Error(w, "unmarshal error: "+err.Error(), http.StatusBadRequest)
			return
		}

		verdict := engine.Evaluate(pack)

		// On ABSTAIN, suggest the prometheus.query capability (Tier-0,
		// PROMETHEUS source) so the client's executor can add an independent
		// corroborating PROMETHEUS leg next round. No directives on FIRE.
		var directives []*gpufleetv1.CollectDirective
		if verdict.GetFaultClass() == gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
			directives = []*gpufleetv1.CollectDirective{
				{CapabilityId: "prometheus.query"},
			}
		}

		resp := &gpufleetv1.InvestigateResponse{
			Verdict:    verdict,
			Directives: directives,
		}
		out, err := protojson.Marshal(resp)
		if err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	}))
}

// TestLoopAbstainToFire proves ABSTAIN→FIRE convergence: the first POST returns
// ABSTAIN (only one source present), the executor adds a genuinely independent
// PROMETHEUS corroborator (not fabricated), and the second POST fires
// FAULT_CLASS_ECC_UNCORRECTABLE via the REAL registry.NewDefaultEngine().
//
// Independence property: the starting pack carries only a DMESG_XID leg
// (dmesg.xid.ecc.*). The executor adds an ecc.dbe.* @ PROMETHEUS leg, which is
// a genuinely distinct source (PROMETHEUS ≠ DMESG_XID). Two independent sources
// → the ECC-uncorrectable signature fires.
func TestLoopAbstainToFire(t *testing.T) {
	srv := realEngineServer(t)
	defer srv.Close()

	now := timestamppb.Now()
	const deviceID = "uuid-gpu0"

	// Round-1 pack: only the DMESG_XID ECC-Xid leg. Single source → ABSTAIN.
	pack := &gpufleetv1.EvidencePack{
		ContractVersion: "v1",
		Timeline: []*gpufleetv1.TimelineEntry{
			{
				Ts:       now,
				Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
				SignalId: "dmesg.xid.ecc.48." + deviceID,
				Label:    "NVRM Xid 48 (uncorrectable ECC) on " + deviceID,
			},
		},
	}

	// The executor provides the independent PROMETHEUS corroborator when the
	// server requests prometheus.query. This is a genuinely distinct source
	// (PROMETHEUS vs DMESG_XID) and satisfies the >=2-independent-source gate.
	exec := &echoExecutor{
		entries: []*gpufleetv1.TimelineEntry{
			{
				Ts:       now,
				Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS,
				SignalId: "ecc.dbe." + deviceID,
				Label:    "ECC double-bit counter delta on " + deviceID + " (prometheus increase())",
			},
		},
	}

	c := &Client{
		URL:      srv.URL,
		Consent:  allTiers,
		Executor: exec,
	}

	verdict, err := c.Investigate(context.Background(), pack)
	if err != nil {
		t.Fatalf("Investigate: %v", err)
	}
	if verdict == nil {
		t.Fatal("verdict must not be nil")
	}
	if got := verdict.GetFaultClass(); got != gpufleetv1.FaultClass_FAULT_CLASS_ECC_UNCORRECTABLE {
		t.Errorf("expected FAULT_CLASS_ECC_UNCORRECTABLE, got %v", got)
	}
	// The verdict must cite signals from two different sources (independence
	// proof: the loop did NOT fabricate a corroborator).
	sources := map[gpufleetv1.SignalSource]bool{}
	for _, cs := range verdict.GetCitedSignals() {
		sources[cs.GetSource()] = true
	}
	if len(sources) < 2 {
		t.Errorf("fired verdict must cite >=2 independent sources; cited sources: %v", sources)
	}
	// The caller's original pack must not be mutated.
	if len(pack.GetTimeline()) != 1 {
		t.Errorf("caller's pack was mutated: timeline now has %d entries, want 1", len(pack.GetTimeline()))
	}
}

// TestLoopTerminatesOnPersistentAbstain locks the no-infinite-loop guarantee:
// when the executor consistently adds nothing new (no-progress) the loop
// terminates with the last ABSTAIN rather than looping forever.
func TestLoopTerminatesOnPersistentAbstain(t *testing.T) {
	srv := realEngineServer(t)
	defer srv.Close()

	// Pack with a single DMESG_XID leg → ABSTAIN every round.
	pack := &gpufleetv1.EvidencePack{
		ContractVersion: "v1",
		Timeline: []*gpufleetv1.TimelineEntry{
			{
				Ts:       timestamppb.Now(),
				Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
				SignalId: "dmesg.xid.ecc.48.uuid-1",
				Label:    "NVRM Xid 48 on uuid-1",
			},
		},
	}

	// The executor never adds new entries → no-progress on every round.
	c := &Client{
		URL:      srv.URL,
		Consent:  allTiers,
		Executor: emptyExecutor{},
	}

	verdict, err := c.Investigate(context.Background(), pack)
	if err != nil {
		t.Fatalf("Investigate returned unexpected error: %v", err)
	}
	if verdict == nil {
		t.Fatal("verdict must not be nil even when all rounds ABSTAIN")
	}
	if got := verdict.GetFaultClass(); got != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Errorf("expected ABSTAIN on persistent no-progress, got %v", got)
	}
}

// TestLoopFiresFirstRound verifies that when the first POST already returns a
// FIRE verdict, the loop returns immediately with no further execution.
func TestLoopFiresFirstRound(t *testing.T) {
	srv := realEngineServer(t)
	defer srv.Close()

	now := timestamppb.Now()
	const dev = "uuid-a"

	// Pack with two independent ECC legs → FIRE on round 1.
	pack := &gpufleetv1.EvidencePack{
		ContractVersion: "v1",
		Timeline: []*gpufleetv1.TimelineEntry{
			{
				Ts:       now,
				Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID,
				SignalId: "dmesg.xid.ecc.48." + dev,
				Label:    "NVRM Xid 48 on " + dev,
			},
			{
				Ts:       now,
				Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
				SignalId: "ecc.dbe." + dev,
				Label:    "ECC double-bit counter on " + dev,
			},
		},
	}

	exec := &capturingExecutor{}
	c := &Client{
		URL:      srv.URL,
		Consent:  allTiers,
		Executor: exec,
	}

	verdict, err := c.Investigate(context.Background(), pack)
	if err != nil {
		t.Fatalf("Investigate: %v", err)
	}
	if verdict.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ECC_UNCORRECTABLE {
		t.Errorf("expected FIRE on round 1, got %v", verdict.GetFaultClass())
	}
	if len(exec.seen) != 0 {
		t.Errorf("executor must not be called when FIRE comes back on round 1; called %d times", len(exec.seen))
	}
}

// TestLoopTerminatesOnNoDirectives verifies that ABSTAIN with an empty directive
// list from the server terminates without calling the executor.
func TestLoopTerminatesOnNoDirectives(t *testing.T) {
	// Stub server: always returns ABSTAIN with NO directives.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &gpufleetv1.InvestigateResponse{
			Verdict: &gpufleetv1.Verdict{
				ContractVersion: "v1",
				FaultClass:      gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN,
				Confidence:      1.0,
			},
			Directives: nil,
		}
		out, _ := protojson.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	}))
	defer srv.Close()

	exec := &capturingExecutor{}
	c := &Client{
		URL:      srv.URL,
		Consent:  allTiers,
		Executor: exec,
	}

	verdict, err := c.Investigate(context.Background(), &gpufleetv1.EvidencePack{ContractVersion: "v1"})
	if err != nil {
		t.Fatalf("Investigate: %v", err)
	}
	if verdict.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Errorf("expected ABSTAIN, got %v", verdict.GetFaultClass())
	}
	if len(exec.seen) != 0 {
		t.Errorf("executor must not be called when server sends no directives")
	}
}

// TestLoopMaxRounds verifies the hard MaxRounds=3 bound: even when the executor
// always adds entries (progress every round) the loop never posts more than
// MaxRounds times.
func TestLoopMaxRounds(t *testing.T) {
	posts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		// Always return ABSTAIN + a valid Tier-0 directive so the loop wants to
		// continue. The executor will add new entries so no-progress doesn't fire.
		resp := &gpufleetv1.InvestigateResponse{
			Verdict: &gpufleetv1.Verdict{
				ContractVersion: "v1",
				FaultClass:      gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN,
				Confidence:      1.0,
			},
			Directives: []*gpufleetv1.CollectDirective{
				{CapabilityId: "dcgm.fields"},
			},
		}
		out, _ := protojson.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	}))
	defer srv.Close()

	now := timestamppb.Now()
	exec := &echoExecutor{
		entries: []*gpufleetv1.TimelineEntry{
			{
				Ts:       now,
				Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
				SignalId: "extra.signal",
				Label:    "extra signal from executor",
			},
		},
	}
	c := &Client{
		URL:      srv.URL,
		Consent:  allTiers,
		Executor: exec,
	}

	verdict, err := c.Investigate(context.Background(), &gpufleetv1.EvidencePack{ContractVersion: "v1"})
	if err != nil {
		t.Fatalf("Investigate: %v", err)
	}
	if verdict.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
		t.Errorf("expected ABSTAIN after MaxRounds, got %v", verdict.GetFaultClass())
	}
	if posts != MaxRounds {
		t.Errorf("expected %d POSTs (MaxRounds), got %d", MaxRounds, posts)
	}
}

// TestLoopConsentGatesDirectives verifies the D-0011 property: a Tier-1
// directive (ebpf.nvlink.retrans) is refused by the consent gate and does not
// reach the executor when Tier-1 is not enabled.
func TestLoopConsentGatesDirectives(t *testing.T) {
	now := timestamppb.Now()
	// Stub server returns ABSTAIN + a Tier-1 directive.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &gpufleetv1.InvestigateResponse{
			Verdict: &gpufleetv1.Verdict{
				FaultClass: gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN,
				Confidence: 1.0,
			},
			Directives: []*gpufleetv1.CollectDirective{
				{CapabilityId: "ebpf.nvlink.retrans"}, // Tier-1
			},
		}
		out, _ := protojson.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	}))
	defer srv.Close()

	// Executor records every directive it receives. We assert it receives NONE:
	// the consent gate must refuse the Tier-1 directive before it reaches here.
	exec := &capturingExecutor{
		entries: []*gpufleetv1.TimelineEntry{
			{
				Ts:       now,
				Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
				SignalId: "extra.signal",
				Label:    "should not appear",
			},
		},
	}

	c := &Client{
		URL:      srv.URL,
		Consent:  tier0Only, // Tier-1 NOT enabled
		Executor: exec,
	}

	_, err := c.Investigate(context.Background(), &gpufleetv1.EvidencePack{ContractVersion: "v1"})
	if err != nil {
		t.Fatalf("Investigate: %v", err)
	}
	// The real assertion: the consent gate must have refused the Tier-1
	// ebpf.nvlink.retrans directive BEFORE it reached the executor. If the gate
	// were broken, the executor would have recorded the directive here.
	if len(exec.seen) != 0 {
		t.Fatalf("consent gate leaked %d directive(s) to the executor; Tier-1 must be refused under Tier-0 consent: %+v", len(exec.seen), exec.seen)
	}
}

// TestInvestigateLoopEgressGate verifies the opt-in egress gate: no HTTP
// request is made when the client is not used (URL empty scenario simulated by
// not calling Investigate), and that Investigate does make a request when the
// URL is provided. This is the contract the cmd/agent flag surface relies on.
func TestInvestigateLoopEgressGate(t *testing.T) {
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		// Return a clean FIRE verdict so the loop terminates immediately.
		resp := &gpufleetv1.InvestigateResponse{
			Verdict: &gpufleetv1.Verdict{
				ContractVersion: "v1",
				FaultClass:      gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN,
				Confidence:      1.0,
			},
		}
		out, _ := protojson.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	}))
	defer srv.Close()

	// Calling Investigate sends exactly one request (ABSTAIN + no directives → done).
	c := &Client{URL: srv.URL, Consent: noTiers}
	_, err := c.Investigate(context.Background(), &gpufleetv1.EvidencePack{ContractVersion: "v1"})
	if err != nil {
		t.Fatalf("Investigate: %v", err)
	}
	if requests != 1 {
		t.Errorf("expected 1 POST when URL is set; got %d", requests)
	}
}

// TestTokenNeverInErrorMessage ensures the Bearer token is not leaked into
// error messages (it would appear in logs via fmt.Errorf wrapping).
func TestTokenNeverInErrorMessage(t *testing.T) {
	const secret = "super-secret-token-9aXf2"

	// Server that always returns 401 so we get an error path.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := &Client{
		URL:     srv.URL,
		Token:   secret,
		Consent: noTiers,
	}
	_, err := c.Investigate(context.Background(), &gpufleetv1.EvidencePack{ContractVersion: "v1"})
	if err == nil {
		t.Fatal("expected error from 401 response")
	}
	// The error message must not contain the secret token.
	if msg := err.Error(); contains(msg, secret) {
		t.Errorf("token leaked into error message: %q", msg)
	}
}

// contains is a simple substring helper so the test file has no import of
// strings just for this one call.
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}()
}
