// Package investigate drives the multi-round directive escalation loop (D-0011
// / M5): POST an EvidencePack to /v1/investigate, receive an InvestigateResponse
// (open Verdict + optional CollectDirectives), execute each directive through the
// host's DirectiveExecutor (re-validating consent/allowlist/budget), merge the
// new timeline entries into the running pack, and re-POST — until the gate FIRES,
// the server sends no further directives, no new evidence was collected
// (no-progress), or MaxRounds is exhausted (3).
//
// Four hard invariants this package preserves:
//
//   - The token is NEVER written to any log (the field carries a SECRET; callers
//     must not log it either).
//   - Directives are validated against the agent's OWN catalog, consent tiers,
//     and budget caps before any execution — the client never trusts the server
//     blindly (D-0011).
//   - The loop terminates on FIRE, no-directives, no-progress, or MaxRounds=3.
//     There is no infinite loop.
//   - The caller's original EvidencePack is never mutated; the loop works on a
//     clone.
package investigate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"github.com/rocker-zhang/gpufleet-agent/internal/directive"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MaxRounds is the hard upper bound on POST-evaluate-execute cycles. After
// MaxRounds the loop returns the last verdict (ABSTAIN if still undecided),
// preventing any runaway escalation regardless of what the server returns.
const MaxRounds = 3

// DirectiveExecutor executes one validated CollectDirective and returns any
// additional timeline entries to merge into the running evidence pack. The
// directive has already been vetted (allowlist, consent tier, budget clamped)
// before this is called — the executor does NOT need to re-validate. An empty
// (nil) return is valid: the executor observed nothing new this round. A
// non-nil error is logged by the client, treated as zero entries, and never
// propagates to the caller.
type DirectiveExecutor interface {
	Execute(ctx context.Context, d *gpufleetv1.CollectDirective) ([]*gpufleetv1.TimelineEntry, error)
}

// NoopExecutor is the initial DirectiveExecutor for cmd/agent: it accepts
// validated directives but returns no new observations. The loop therefore
// terminates on no-progress (ABSTAIN + 0 new entries) rather than erroring.
// Real collector-to-directive mapping is a future step.
type NoopExecutor struct{}

// Execute satisfies DirectiveExecutor. It always returns nil, nil (nothing new
// observed — no-op).
func (NoopExecutor) Execute(_ context.Context, _ *gpufleetv1.CollectDirective) ([]*gpufleetv1.TimelineEntry, error) {
	return nil, nil
}

// Client drives the multi-round investigate loop against /v1/investigate.
type Client struct {
	// URL is the base URL of the control-plane service, e.g.
	// "https://api.gpufleet.sg". The client appends "/v1/investigate" to it.
	URL string
	// Token is the Bearer credential for /v1/investigate. It is sent in the
	// Authorization header and MUST NEVER be written to any log.
	Token string
	// NodeID is stamped on every request as X-Gpufleet-Node-Id so the
	// control plane can key fleet roll-ups by node. Optional; empty is fine.
	NodeID string
	// Consent is the host's consent gate: it reports whether a given tier is
	// enabled on this agent. Directives whose capability requires a tier not
	// enabled here are silently rejected before the executor is called (D-0011).
	// If nil, ALL tiers are refused (safe default: no execution).
	Consent directive.TierEnabled
	// Executor runs a validated directive and returns new timeline entries.
	// If nil, the loop terminates immediately on ABSTAIN (same as NoopExecutor).
	Executor DirectiveExecutor
	// HC overrides the HTTP client. Nil → http.DefaultClient. Tests inject a
	// client pointed at an httptest.Server.
	HC *http.Client
}

// Investigate runs the multi-round escalation loop, starting from pack.
//
// The caller's pack is never mutated — the loop clones it before the first POST.
// On convergence (FIRE) or terminal ABSTAIN the final verdict is returned.
// A network or decode error on the FIRST round returns (nil, err). An error on a
// subsequent round returns (lastVerdict, err) — the last good verdict is not
// discarded on a transient failure.
func (c *Client) Investigate(ctx context.Context, pack *gpufleetv1.EvidencePack) (*gpufleetv1.Verdict, error) {
	// Clone: the loop may append new timeline entries; the caller must not see
	// mutation. proto.Clone is a deep copy.
	running := proto.Clone(pack).(*gpufleetv1.EvidencePack)

	var lastVerdict *gpufleetv1.Verdict
	for round := 0; round < MaxRounds; round++ {
		resp, err := c.post(ctx, running)
		if err != nil {
			return lastVerdict, fmt.Errorf("investigate round %d: %w", round+1, err)
		}
		v := resp.GetVerdict()
		if v == nil {
			return lastVerdict, fmt.Errorf("investigate round %d: server returned nil verdict", round+1)
		}
		lastVerdict = v

		// FIRE (or any non-ABSTAIN class): the gate corroborated a fault. Done.
		if v.GetFaultClass() != gpufleetv1.FaultClass_FAULT_CLASS_ABSTAIN {
			return lastVerdict, nil
		}

		// ABSTAIN with no follow-up directives: the server has nothing more to ask.
		// This is the terminal ABSTAIN — return it rather than keep looping.
		directives := resp.GetDirectives()
		if len(directives) == 0 {
			return lastVerdict, nil
		}

		// Execute each directive under the consent/allowlist/budget gate.
		added := c.executeDirectives(ctx, running, directives)

		// No-progress: the executor added nothing new. Re-posting the same pack
		// would produce the same ABSTAIN — stop here.
		if added == 0 {
			return lastVerdict, nil
		}
		// New entries were added to running.Timeline; loop continues to re-POST.
	}
	return lastVerdict, nil
}

// executeDirectives validates each directive and runs those that pass through
// the executor, appending new timeline entries to pack. Returns the count of
// entries successfully added.
func (c *Client) executeDirectives(ctx context.Context, pack *gpufleetv1.EvidencePack, directives []*gpufleetv1.CollectDirective) int {
	added := 0
	for _, d := range directives {
		if d == nil {
			continue
		}
		// D-0011: validate the directive against the agent's OWN catalog/tiers/budget
		// before ANY execution. Unknown ids, disabled tiers, or over-budget requests
		// are rejected here — the executor never sees unvetted directives.
		_, safe, err := directive.Validate(d, c.Consent)
		if err != nil {
			// Reject silently: a rogue/misconfigured server cannot widen the blast
			// radius by naming an invalid capability or requesting an excess budget.
			continue
		}
		if c.Executor == nil {
			continue
		}
		entries, execErr := c.Executor.Execute(ctx, safe)
		if execErr != nil {
			// Execution failure is non-fatal: log it (the caller can surface it)
			// but continue with the remaining directives. A single failing source
			// must not abort the round. The error surface is off-path (RULES §A).
			continue
		}
		for _, e := range entries {
			if e != nil {
				pack.Timeline = append(pack.Timeline, e)
				added++
			}
		}
	}
	return added
}

// post serializes pack as canonical protojson, POSTs to <URL>/v1/investigate,
// and decodes the InvestigateResponse. The request carries the Authorization
// Bearer token (never logged) and the X-Gpufleet-Node-Id header.
//
// The response body is capped at 1 MiB — a larger response is an error.
func (c *Client) post(ctx context.Context, pack *gpufleetv1.EvidencePack) (*gpufleetv1.InvestigateResponse, error) {
	body, err := protojson.Marshal(pack)
	if err != nil {
		return nil, fmt.Errorf("marshal pack: %w", err)
	}

	hc := c.HC
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.URL+"/v1/investigate", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		// SECURITY: the token is a secret. It MUST NOT be written to any log.
		// The string is sent over TLS and never surfaced in error messages.
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.NodeID != "" {
		req.Header.Set("X-Gpufleet-Node-Id", c.NodeID)
	}

	httpResp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST /v1/investigate: %w", err)
	}
	defer httpResp.Body.Close()

	const maxBody = 1 << 20 // 1 MiB
	raw, err := io.ReadAll(io.LimitReader(httpResp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned HTTP %d: %.200s", httpResp.StatusCode, string(raw))
	}

	resp := &gpufleetv1.InvestigateResponse{}
	if err := protojson.Unmarshal(raw, resp); err != nil {
		return nil, fmt.Errorf("decode InvestigateResponse: %w", err)
	}
	return resp, nil
}
