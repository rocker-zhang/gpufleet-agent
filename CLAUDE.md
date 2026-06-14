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

## 5c. 静态 device-spec + 查询/标签覆盖 (static spec + override — TASK-0038)

On a **vanilla** dcgm-exporter + Prometheus box the cost-rate series
(`gpufleet_device_cost_usd_per_hour`) and the tensor-peak field
(`DCGM_FI_DEV_TENSOR_PEAK_FLOPS`) **do not exist**, so `$/hr` and **MFU silently
degrade to 0**. `$/hr` is inherently an operator **price** input; peak-FLOPS for a
known GPU model is a **static spec**. Supply them as a static device-spec so MFU +
`$/hr` render real — without rebuilding.

```sh
# A10 box: real DCGM tensor-active, no peak/cost series → fill from a spec file.
agent -serve \
  --dcgm-exporter-url  http://127.0.0.1:9400/metrics \
  --device-spec-file   /etc/gpufleet/device-spec.json \
  --node $NODE_NAME

# Homogeneous box shortcut (no JSON): one peak + one rate for every device.
agent -serve --dcgm-exporter-url http://127.0.0.1:9400/metrics \
  --peak-tflops 125 --cost-usd-per-hour 1.20 --node $NODE_NAME
```

Example `device-spec.json` (per-GPU-model; see `testdata/device-spec-a10.json`):

```json
{ "NVIDIA A10": { "peak_tflops": 125, "cost_usd_per_hour": 1.20 } }
```

Per-UUID pinning (wins over model) uses the explicit shape; both may coexist:

```json
{
  "by_uuid":  { "GPU-abc123": { "peak_tflops": 312, "cost_usd_per_hour": 4.10 } },
  "by_model": { "NVIDIA A10": { "peak_tflops": 125, "cost_usd_per_hour": 1.20 } }
}
```

Model matching is **case-insensitive** and tolerant of a leading `NVIDIA ` prefix
(`A10` ≡ `NVIDIA A10`). Match order: UUID → named model → `*` wildcard (the quick
shortcut). A zero/omitted field fills nothing and keeps degrading.

**Semantics (RULES §B):** the spec is an **explicit operator input**, so using it
is not fabrication — but every spec-sourced value is **stamped** in pack provenance
so the origin stays auditable:

| provenance key | meaning |
|----------------|---------|
| `<src>.spec.peak.source = static-spec` | a peak was filled from the spec |
| `<src>.spec.cost.source = static-spec` | a `$/hr` rate was filled from the spec |
| `<src>.spec.achieved_flops.source = derived:tensor-active*static-spec-peak` | achieved-FLOPs was derived from the **real** tensor-active series × the spec peak (the same identity as the default PromQL) |
| `<src>.spec.filled_devices = <uuid,...>` | which devices the spec touched |

A **real series ALWAYS wins** over the spec (the spec only fills a gap a real
source left). With **no spec AND no real series** the field still **degrades**
(`mfu`/`cost` degrade-marks), never invented. An **empty spec is a transparent
passthrough** — the no-spec mock/real path is byte-for-byte unchanged.

### Query / label override

Align to a non-default dcgm-exporter schema discovered in recon **without
rebuilding** — override the PromQL expressions and/or the identity label keys.
Each flag has an env equivalent; an unset query field falls back to its
`DefaultPromQueries` value (override one, keep the rest), an unset label key
falls back to the common DCGM-exporter default.

| flag | env | meaning (default) |
|------|-----|-------------------|
| `--device-spec-file` | `GPUFLEET_DEVICE_SPEC_FILE` | static device-spec JSON (per-model or per-UUID). |
| `--peak-tflops` | `GPUFLEET_PEAK_TFLOPS` | homogeneous-box peak TFLOP/s (wildcard `*` entry). |
| `--cost-usd-per-hour` | `GPUFLEET_COST_USD_PER_HOUR` | homogeneous-box `$/hour` (wildcard `*` entry). |
| `--query-tensor-active` | `GPUFLEET_QUERY_TENSOR_ACTIVE` | PromQL for tensor-active (`DCGM_FI_PROF_PIPE_TENSOR_ACTIVE`). |
| `--query-achieved-flops` | `GPUFLEET_QUERY_ACHIEVED_FLOPS` | PromQL for achieved FLOP/s (`…TENSOR_ACTIVE * …TENSOR_PEAK_FLOPS`). |
| `--query-peak-flops` | `GPUFLEET_QUERY_PEAK_FLOPS` | PromQL for peak FLOP/s (`DCGM_FI_DEV_TENSOR_PEAK_FLOPS`). |
| `--query-cost-per-hour` | `GPUFLEET_QUERY_COST_PER_HOUR` | PromQL for `$/hour` (`gpufleet_device_cost_usd_per_hour`). |
| `--query-job-owner` | `GPUFLEET_QUERY_JOB_OWNER` | PromQL for device→job (`gpufleet_device_job`). |
| `--label-uuid` | `GPUFLEET_LABEL_UUID` | device-UUID label key (`UUID`). |
| `--label-node` | `GPUFLEET_LABEL_NODE` | hostname label key (`Hostname`). |
| `--label-model` | `GPUFLEET_LABEL_MODEL` | model label key (`modelName`). |
| `--label-job` | `GPUFLEET_LABEL_JOB` | job label key (`job`). |

```sh
# Example: a box whose UUID label is `gpu` and whose cost series has a custom name.
agent -serve --prometheus-url http://prometheus:9090 \
  --label-uuid gpu \
  --query-cost-per-hour 'my_org_gpu_price_usd_hour' \
  --device-spec-file /etc/gpufleet/device-spec.json
```

## 5e. 内置 GPU 峰值查表 (built-in peak-FLOPS table — TASK-0044)

On a **vanilla** dcgm-exporter box the tensor-peak field
(`DCGM_FI_DEV_TENSOR_PEAK_FLOPS`) **does not exist**, so MFU silently degrades and
the operator must hand-supply `--peak-tflops 125`. But the peak FLOPs of a known
datacenter GPU is a **published per-(model × precision) datasheet constant**, and
the agent already sees the DCGM `modelName` label (e.g. `"NVIDIA A10"`). So it can
**auto-resolve** the peak from the model name — **zero-config real MFU**:

```sh
# A10 box, NO --peak-tflops: the built-in table fills peak=125 from modelName.
agent -serve --dcgm-exporter-url http://127.0.0.1:9400/metrics --node $NODE_NAME
```

### Strict precedence (RULES §B — degrade-not-fabricate)

```
real telemetry peak (DCGM/Prom series, if present)
  > operator-explicit --peak-tflops / --device-spec-file
  > built-in table (by modelName)
  > DEGRADE (peak stays unknown, MFU degrades, never fabricated)
```

The table is the **LAST resort before degrade**: `PeakTableCollector` wraps the
chain **outside** `SpecFillCollector`, so it only fills a peak that neither a real
series nor an explicit operator spec supplied. An **unknown model DEGRADES** (the
peak stays unknown and is marked) — it is **never guessed**.

### Precision basis (honesty)

Every entry is the **NVIDIA datasheet FP16/BF16 DENSE tensor** peak — **NOT** the
2× sparse number, NOT FP8/FP4, NOT FP32/FP64. This single, documented basis
matches how the cost wedge computes MFU (achieved tensor FLOPs ÷ peak). The basis
is stamped into provenance so a reviewer can audit both the origin AND the
precision assumption.

### Coverage (FP16/BF16 dense tensor TFLOP/s; source: NVIDIA datasheets)

| modelName (matched) | canonical | TFLOP/s |
|---|---|---|
| A10 | A10 | 125 |
| A100 (40GB / 80GB, any SKU) | A100-40GB / A100-80GB | 312 |
| H100 SXM / H100 PCIe / H100 NVL | H100-SXM / H100-PCIe / H100-NVL | 989.4 / 756 / 835 |
| H200 | H200 | 989.4 |
| GH200 (Grace-Hopper, shares H100 GH100 die) | GH200 | 989.4 |
| L4 / L40 / L40S | L4 / L40 / L40S | 121 / 181 / 362 |
| V100 (FP16) | V100 | 125 |
| T4 (FP16) | T4 | 65 |
| RTX A6000 | A6000 | 155 |
| B200 | B200 | 2250 |

**GB10 / DGX Spark is DELIBERATELY ABSENT.** The widely-quoted "1 petaFLOP" GB10
figure is the **FP4-with-sparsity** marketing number — not FP16/BF16 dense — so
adding it would break this table's basis. NVIDIA publishes no confident
FP16/BF16-dense per-GPU datasheet figure for GB10, so per RULES §B
(degrade-not-fabricate) a GB10 box matches **nothing** → peak degrades → the
operator supplies `--peak-tflops`. An entry is added only once a real
FP16/BF16-dense datasheet number is published.

Model matching is **case-insensitive** and tolerant of a leading `NVIDIA `/`Tesla
`/`RTX ` vendor prefix and of **SKU suffixes** (`A100-SXM4-80GB` → `A100-80GB`,
`Tesla T4` → `T4`, `H100 80GB HBM3` → `H100-SXM`; `L40S` prefers `L40S` over
`L40`). H100 defaults to the SXM form factor unless the SKU says PCIe, with `NVL`
its own distinct per-GPU dense figure (835, not the 756 PCIe board).

### FLOPs only — **$/hr stays operator-supplied**

The table carries **NO `$/hr`**: a price is an operator input (a datasheet cannot
know what a customer pays), so cost stays operator-supplied
(`--device-spec-file` / `--cost-usd-per-hour`) and **degrades** (device is
*unpriced*, never `$0`-faked) when absent. So a zero-config A10 box renders **real
MFU but an unpriced wedge** until the operator supplies a rate.

### Provenance (auditable origin + precision basis)

| provenance key | meaning |
|----------------|---------|
| `<src>.builtin.peak.source = builtin-table:<model>@fp16-dense` | a peak was auto-resolved from the table for `<model>` at the FP16/BF16 dense basis |
| `<src>.builtin.filled_devices = <uuid,...>` | which devices the table touched |
| `<src>.spec.achieved_flops.source = derived:tensor-active*builtin-table-peak` | achieved-FLOPs was derived from the **real** tensor-active series × the table peak (the same identity as the default PromQL) |

### Flag

| flag | env | meaning (default) |
|------|-----|-------------------|
| `--builtin-peak-table` | `GPUFLEET_BUILTIN_PEAK_TABLE` | auto-resolve a missing peak from the built-in datasheet table by modelName (**default `true`**). Operator `--peak-tflops`/`--device-spec-file` and real telemetry still win. Set `false` for pure degrade-not-fabricate. |

The table is applied **only on the REAL telemetry path**, NOT the mock default:
the mock's synthetic `A10` devices include an intentionally peak-degraded device
the demo relies on, so the **mock default stays byte-for-byte unchanged**
(demo1 / CI regression green). NO `proto/` change (peak still rides the existing
`DeviceJobMapping.PeakTflops`); FLOPs-only, no egress/Verdict edits.

## 5d. 数据新鲜度 / staleness (data freshness — TASK-0040)

A read-only monitor must **never present stale data as current**. There are TWO
distinct latencies; the agent handles them differently:

- **(a) DCGM profiling sample window (~25–30s) — UPSTREAM, NOT measured here.**
  DCGM's profiling fields (`PIPE_TENSOR_ACTIVE`, `SM_ACTIVE`, `DRAM_ACTIVE`) are
  themselves averaged over DCGM's own ~25–30s collection cycle. That latency is a
  property of the exporter, **upstream of the agent** — the agent reads the value
  the exporter publishes and **cannot observe** how old that internal sample is.
  We only **document** it: even a perfectly fresh scrape carries a number that is
  up to ~25–30s averaged. This is NOT what `stale` below measures.
- **(b) Agent-side freshness — MEASURED here.** How long since the agent last
  **SUCCESSFULLY** scraped its source. The agent owns this clock, so it reports
  it. If the exporter goes unreachable / collection keeps failing, the daemon
  keeps serving the **last-known** window (never blanked, never fabricated —
  RULES §A) but marks it `stale=true` once its age passes the threshold, so a
  consumer presents it as stale, not live (RULES §B).

**Threshold.** Configurable via `--staleness-after` / `GPUFLEET_STALENESS_AFTER`.
Unset (`0`) derives `max(3×interval, 5s)` — three missed collections, floored so
a fast interval does not flap to stale on normal jitter.

**Mechanics.** The daemon publishes a new `State` only on a *successful*
collection, so `RefreshAt` is the last-successful-collection time. A failed cycle
(every source erred, or a hard Normalize/cost error) bumps a consecutive-failure
streak and a reason, **leaves the prior State in place**, and never advances the
refresh counter. `age = now − collected_at`; `stale = age > staleness-after`. A
later success resets the streak/reason and serves live again. A *partial*
collection that still publishes a window is **fresh** (not stale) — only a
full-cycle failure ages the data.

**Where it surfaces (untyped JSON only).** `/cost` and `/healthz` are the agent's
own untyped JSON (NOT proto — §5a), so freshness is added there in-scope:

`GET /cost` gains top-level fields (the per-device `DeviceCost`/`JobCost` shapes
are **unchanged**, so the §5a golden still decodes):

| field          | type   | meaning |
|----------------|--------|---------|
| `collected_at` | string | RFC3339 time of the last SUCCESSFUL collection |
| `age_seconds`  | float  | `now − collected_at`, seconds |
| `stale`        | bool   | `age_seconds` exceeded `--staleness-after`, OR `never_collected` |
| `never_collected` | bool | no successful metrics scrape EVER (most stale; empty `devices`, 200 not 503) — TASK-0041 |
| `stale_reason` | string | provenance when stale (e.g. "N consecutive collection failure(s): …" / "never collected: …"); empty when fresh |

`GET /healthz` reflects `last_success_at`, `age_seconds`, `stale`,
`never_collected`, `stale_reason`, `consec_failures`. `ok` stays **true** even
when stale — the agent process is healthy and off-path; staleness is a
*data*-freshness signal, not a liveness failure.

**Never-collected = MOST stale (TASK-0041).** A distinct, worse case from
"was-fresh-then-stale": the agent has **never once** successfully scraped metrics
(e.g. the exporter was unreachable from startup), so there is **no last-known
window to serve at all**. A never-collected agent is the *most* stale state and
is **never** reported fresh (RULES §B). The lab found (#29) the opposite bug:
`/healthz` reporting `stale:false` + `last_success_at:0001-01-01`(zero time) +
`age_seconds:0`, and `/cost`→503. Corrected semantics:

- `Freshness()` returns `stale=true` + `never_collected=true` (`HasData=false`)
  with a populated `reason` ("never collected: no successful metrics scrape since
  startup (…cause…)"). `age` is measured **since daemon startup**, never a
  misleading `0` that would read as "just collected".
- `/healthz`: `stale=true`, `never_collected=true`, **no** `last_success_at`
  (omitted — never a zero-time fake), `age_seconds` = since-startup.
- `/cost`: returns **200 (not 503)** with **empty** `devices`/`jobs`,
  `stale=true`, `never_collected=true`, and a `stale_reason` — a self-consistent,
  machine-readable empty state. `collected_at` is omitted (there is none).

The cli renders this as a clear **"NO DATA — the agent has not collected any GPU
metrics yet"** message + the agent's reason, never a blank table and never a raw
HTTP error. For resilience it also degrades an **older** agent's `503` on `/cost`
into the same never-collected empty state (carrying the agent's body as the
reason). A reachable-but-empty agent (never-collected, `200`/`503`) is kept
distinct from an **unreachable** agent (transport error ⇒ cli still says "cannot
reach the agent"). The `/cost`/`/healthz` `never_collected` field is **additive**
untyped JSON (NOT proto — §5a/§D): no `proto/` change, so `/signals` is untouched.

**Proto note (§D, ABSTAIN).** Freshness is exposed on the agent's untyped `/cost`
+ `/healthz` only. Putting it into the proto-typed `/signals` EvidencePack would
require a `proto/` contract change — **out of scope; ABSTAIN** (orchestrator
decision). `/signals` is left untouched.

The `cli` viewer renders a `data age: Ns` line and, on `stale`, a prominent
`*** STALE ***` marker + the agent's reason + a "do not treat as current" note,
while still showing the last-known values. cli passes the agent's verdict through
verbatim (it does not recompute the threshold) and stays a read-only HTTP viewer.

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
