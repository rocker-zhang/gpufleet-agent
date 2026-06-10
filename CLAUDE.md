# CLAUDE.md — gpufleet-agent (module session rules)

You are a Claude session **scoped to this repo only** (`gpufleet-agent`). This
is an OPEN module (Apache-2.0). Your edits are **confined to this repo**.

## What this module is

The read-only collector/sidecar. Reads DCGM/Prometheus/dmesg/NCCL and emits a
normalized `Evidence` struct. Default build is mock (no GPU); NVML is isolated
behind `//go:build gpu` and is lab-only.

## Hard boundaries (do not cross)

- **Read-only & off-critical-path.** This code must NEVER control, orchestrate,
  checkpoint, reset, set clocks/power, or otherwise mutate a GPU, and must never
  run inside a job-execution path. Even in the `gpu`-tagged path, only read-only
  NVML queries are allowed.
- **Evidence-only egress.** The only thing this agent may send to the control
  plane is a structured evidence pack (a normalized signal window). NEVER send
  prompts, playbooks, heuristics, or fault adjudication. The verdict is decided
  server-side; the agent only receives it.
- **No LLM here.** LLM narration lives in the closed control plane. Do not add
  any Claude/Anthropic API call to this repo.
- **Edits confined here.** Need a change in `semantics`, `rca`, `cli`, or the
  control plane? ABSTAIN and report a blocker. Do not reach across.
- **`proto/` is READ-ONLY.** Read the vendored contracts; never edit them. A
  needed contract change = a *contract change proposal* blocker for the
  orchestrator, not an edit here.
- **No externally-sourced content.** No copied code/data or proprietary
  error-code semantics from any prior work.

## If you are blocked

File a short blocker (what you needed, which module/contract it touches) and
stop. No cross-repo workarounds.

## Definition of done

`go test -race ./...` (default mock build) and `go vet ./...` pass; the
`-tags gpu` build still compiles.
