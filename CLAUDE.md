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

## 5a. `/cost` 本地只读 wire 契约 (TASK-0032)

`GET /cost` is the agent's LOCAL read-only standalone cost-wedge endpoint
(D-0010 Endpoint 1). It is **untyped JSON, NOT a proto message** — `/cost` is a
local read-only API surface, not a controlplane contract, so it is intentionally
NOT anchored in `proto/` (a proto upgrade would be an orchestrator contract
decision, out of scope; RULES §D). Because the OPEN `cli` viewer must NOT link
the agent Go module, it hand-copies these DTOs; this section is the single
source of truth that both sides pin to, plus a committed golden fixture
(`testdata/cost_golden.json`, byte-identical under `cli/testdata/`) and a
contract test on each side decoding it (so the hand-copied DTO can't silently
drift).

```
GET /cost  →  200  application/json
{
  "devices": [ <DeviceCost>, ... ],   // per-device wedge, sorted by uuid
  "jobs":    [ <JobCost>,    ... ]     // per-job aggregate, sorted by job_id
}
```

`DeviceCost` fields (all always present; no `omitempty`):

| field             | type    | meaning |
|-------------------|---------|---------|
| `uuid`            | string  | device UUID |
| `node`            | string  | node name |
| `mfu`             | float   | model-FLOP-utilization fraction [0,1] |
| `tensor_active`   | float   | tensor-active fraction [0,1] |
| `idle_fraction`   | float   | `1-mfu` clamped [0,1]; basis for waste |
| `cost_usd`        | float   | device spend over the window, USD |
| `wasted_usd`      | float   | **per-WINDOW** waste = `cost_usd * idle_fraction`, USD |
| `usd_per_hour`    | float   | **per-HOUR** burn RATE = `cost_per_hour * idle_fraction`, USD/hr (semantics `CostImpact.UsdPerHour`) |
| `priced`          | bool    | `CostImpact.Computed`: a $/hr rate was known |
| `low_utilization` | bool    | deterministic LOW_UTILIZATION rule fired (informational) |

`JobCost` fields:

| field          | type   | meaning |
|----------------|--------|---------|
| `job_id`       | string | job id |
| `wasted_usd`   | float  | aggregate per-window waste across the job's priced devices |
| `usd_per_hour` | float  | aggregate per-hour burn rate across the job's priced devices |
| `priced`       | bool   | any device in the job was priced |
| `devices`      | int    | device count in the job wedge |

Semantics that consumers (cli) MUST honor:
- `wasted_usd` (a window slice) and `usd_per_hour` (a rate) are **distinct**.
  For a sub-hour window an idle device's `usd_per_hour` **exceeds** its
  `wasted_usd`. A healthy device has both at `0`.
- When `priced == false`, the `$` fields (`cost_usd`, `wasted_usd`,
  `usd_per_hour`) are `0` and carry **no meaning** ("could not compute", never
  "free"). The cli renders a **degrade mark, not `$0`** for these.
- The agent **omits** (does not fabricate) a device whose MFU inputs were
  degraded; such a device appears in `/signals` mappings but not in `/cost`
  (the cli surfaces that as an `mfu` degrade mark).

This is a **read-only透出** of `semantics.CostImpact` — no wedge compute change,
no write-back (RULES §A).

## 5b. 指向真实端点 (point at real Prometheus/DCGM — TASK-0037)

By default `agent -serve` runs the **mock** collectors (demo1 / CI, GPU-less). To
read **real** telemetry in the lab, pass endpoint flags (or the equivalent env
vars). HTTP scrape needs **no NVML** — the real `PrometheusCollector` /
`DCGMExporterCollector` compile in the default `!gpu` build, so real-endpoint
collection works **without `-tags gpu`** (NVML/kmsg is only for the gpu build).

```sh
# Real read-only collection on the default (no-NVML) build:
agent -serve \
  --prometheus-url     http://prometheus:9090 \
  --dcgm-exporter-url  http://127.0.0.1:9400/metrics \
  --node $NODE_NAME
```

| flag | env | meaning |
|------|-----|---------|
| `--prometheus-url` | `GPUFLEET_PROMETHEUS_URL` | EXISTING Prometheus root; read-only PromQL instant queries, **zero new scrape load** (preferred metrics source). |
| `--dcgm-exporter-url` | `GPUFLEET_DCGM_EXPORTER_URL` | local DCGM-exporter `/metrics`; read-only fallback scrape. |
| `--collectors` | `GPUFLEET_COLLECTORS` | `auto` (default: real if any endpoint given, else mock) \| `mock` \| `real`. |
| `--nccl-log` | `GPUFLEET_NCCL_LOG` | NCCL log file to tail read-only (real collectors only). |
| `--profiling-burst` | `GPUFLEET_PROFILING_BURST` | opt into high-res DCGM profiling scrapes (default off). |
| `--profiling-cap` | `GPUFLEET_PROFILING_CAP` | min interval between profiling-burst scrapes (the off-path frequency cap, D-0009). |

When an endpoint is given the daemon wires the REAL `MetricsChain`
(Prometheus-first → DCGM fallback, `NewMetricsChain`) + the log/event collector,
replacing the mock `DefaultCollectors`. **With NO endpoint flags the behavior is
unchanged** (mock default, demo1 green). An **unreachable/garbage endpoint**
degrades through the existing chain (mark+degrade) — it **never crashes** the
daemon or affects a job (RULES §A). Default PromQL expressions assume
DCGM-exporter labeling (`DefaultPromQueries`); override via the package
`RuntimeConfig.Queries` if your labels differ. Everything here is **read-only**
config + read endpoints — zero write-back.

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
