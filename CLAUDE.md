# agent — module brief (CLAUDE.md)

## 1. 身份
- **class:** OPEN (Apache-2.0)
- **language:** Go
- **kind:** daemon (per-node DaemonSet sidecar, always-on)
- **purpose (one line):** the customer-side **data plane** — collect/normalize host signals, run the deterministic gate to a local Verdict, assemble the evidence pack, and be the **sole** HTTPS egress + **sole** Verdict receiver.
- **on-path|bypass|shared-lib:** **ON-PATH** (the one necessary data-plane component; the single outbound HTTPS originates here).

## 2. 在系统里的位置
See [../ARCHITECTURE.md](../ARCHITECTURE.md) §1 (data flow) and D-0008/0009/0010/0011 in `ops/DECISIONS.md`.

**Consumes (read-only):**
- **proto** contracts: `SignalSchema` (normalized signal), shared **gate class enum + signature IDs**, **Verdict** envelope, **PluginProto** capability/Describe contract, and the controlplane fleet-aggregation envelope.
- **Host sources** (D-0009): customer **Prometheus** (PromQL HTTP API, DEFAULT for metrics, zero new exporter load) → FALLBACK **DCGM-exporter** scrape (per-node, when no Prom / missing fields / higher-res); **dmesg/XID** (tail `/dev/kmsg`), **NCCL**, **nvidia-smi/NVML** polled locally (fault-signal gap exporters don't cover).
- **semantics** (compile-time lib): device→job mapping, MFU / Tensor-active / $cost math.
- **rca** (compile-time lib): signature engine, ≥2-signal gate, public playbooks → local Verdict (ABSTAIN | class).

**Produces:**
- Normalized `SignalSchema` windows + **local Verdict**.
- **Evidence pack** (signal window + local verdict + context) — uploaded over the single HTTPS call.
- **Local read-only API** (unix socket / localhost): signals + verdict, consumed by the OPEN **cli** bypass viewer (D-0010 Endpoint 1, single-node, free). The cli NEVER reaches the controlplane through the agent.

**Edges:**
- agent → controlplane: agent **always initiates** outbound HTTPS; evidence pack up, Verdict + declarative **directive** down (D-0011). **Zero inbound** to the customer node.
- cli → agent: local read only (off-path).

## 3. 继承的红线
Inherits [../RULES.md](../RULES.md). Module-specific hard lines:
- **Read-only & off-critical-path.** NEVER control/orchestrate/checkpoint/reset/set clocks/power or mutate a GPU; never run in a job-execution path. Even under `//go:build gpu`, only read-only NVML queries.
- **SOLE evidence-pack assembler + SOLE HTTPS egress + SOLE Verdict receiver.** The only thing sent to the controlplane is the structured evidence pack. NEVER send prompts, playbooks, heuristics, or fault adjudication; the verdict is decided server-side and only received here.
- **Capability executor (D-0011).** Runs ONLY pre-shipped, **signed, vetted** collectors selected by a **declarative directive** from the controlplane's response. The controlplane NEVER pushes code/playbooks/bytecode; it selects from the agent's FIXED, OPEN, customer-auditable catalog. eBPF/perf probes are read-only observers **bundled in this open agent**.
- **Consent tiers (D-0011).** Validate every directive against an allowlist. **Tier-0** unprivileged default (DCGM/Prom/dmesg). **Tier-1** needs `CAP_BPF`/`CAP_PERFMON` + explicit customer opt-in (eBPF/perf). The controlplane can only request a tier the customer enabled. Agent is the **sole policy enforcer**.
- **Resource guards + kill-switch** on every collector (max duration/CPU/maps, auto-detach) so a probe can never stall a node.
- **Profiling cap (D-0009).** DCGM *profiling* fields (`SM_ACTIVE`, `PIPE_TENSOR_ACTIVE`, `DRAM_ACTIVE`) default to the customer's existing scrape interval; high-res burst is **opt-in with a frequency cap** (this is "off-critical-path" in practice).
- **Outbound-only.** Agent always initiates; the directive rides in the controlplane's response; zero inbound connection to the node.
- **No LLM here.** Narration lives in the closed controlplane. No Claude/Anthropic API call in this repo.
- **`proto/` is READ-ONLY.** A needed contract change = a *contract change proposal* blocker for the orchestrator, not an edit here.
- **No externally-sourced content.** No copied code/data or proprietary error-code semantics from any prior work.

## 4. 当前任务 & 里程碑焦点
Cards in `ops/BOARD.md` touching this module:
- **TASK-0019** — agent owns evidence-pack assembly + sole HTTPS egress + local read-only API (BLOCKING; D-0008 data-plane correction).
- **TASK-0018** — independence judged by **source**, not declared field (P0; gate uses rca, but source-tagging of signals happens at collection here).
- **TASK-0022** — standalone-useful test: open clone works with no control-plane edge (M2).
- Consumes **TASK-0017** (proto v0.1.0 tag/gen) and **TASK-0021** (proto shared gate enum + signature IDs).

**Current milestone (M2):** read-only agent collects a real evidence pack from Prometheus/DCGM (+mock), exposes the local API for the cli wedge, **standalone-useful with no control-plane**.

## 5. 构建与测试
`source ../.envrc` first (project-local toolchain; tools/caches under `./.tools` & `./.cache`; per-repo local git identity; no global pollution — RULES §J).
- Build/test (default mock, no GPU): `go build ./... && go test -race ./... && go vet ./...`
- GPU path must still compile: `go build -tags gpu ./...` (NVML isolated behind `//go:build gpu`, lab-only, read-only queries).
- **CI (one line):** mock build runs `go test -race` + `go vet`; the `-tags gpu` build is compile-checked.

## 6. session 工作规则
- Edits **confined to this repo** (`agent/`). `proto/` read-only.
- Need a change in `semantics`, `rca`, `cli`, `proto`, or the controlplane? **ABSTAIN and file a short blocker** (what you needed, which module/contract). No cross-repo workarounds.
- Provenance: personal hardware/time only; no externally-sourced content.

## 7. 模块路线图 (mirror ROADMAP.md)
- **M1** — buildable scaffold: collectors interface, mock source, `SignalSchema` emit, green CI.
- **M2** — read-only Prom/DCGM collection → evidence pack; local read-only API for cli; standalone-useful (no control-plane). **[current]**
- **M3** — local ≥2-signal gate via rca → local Verdict; dmesg/XID + NCCL collectors; lab-injected fire/abstain.
- **M4** — bench harness wiring; deterministic evidence-grounding under regression gate.
- **M5** — sole HTTPS egress to controlplane; Verdict receiver; declarative-directive executor + consent tiers + resource guards (D-0011).
- **M6** — partner hardening: real-fleet evidence packs, profiling-cap + kill-switch validation.
