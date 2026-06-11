# agent — module roadmap (ROADMAP.md)

Module-local breakdown of the fleet roadmap (`../ROADMAP.md` / `ops/PLAN.md`).
The **agent** is the OPEN, on-path data plane: collect → normalize → gate (local
Verdict) → evidence pack → sole HTTPS egress + sole Verdict receiver + capability
executor. Read-only, off-critical-path, no LLM.

| Milestone | This module delivers | Exit criteria |
|-----------|----------------------|---------------|
| **M1** scaffold & contracts | Collector plugin interface (`PluginProto`-shaped); mock source; normalize to proto `SignalSchema`; default no-GPU build with NVML isolated behind `//go:build gpu`. | `go test -race ./...` + `go vet ./...` green on mock; `-tags gpu` compiles; emits a valid `SignalSchema`. |
| **M2** 只读 agent + CLI 楔子 (the money story) | **D-0009 ingestion:** Prometheus PromQL collector (DEFAULT) + DCGM-exporter scrape (FALLBACK); profiling fields (`SM_ACTIVE`/`PIPE_TENSOR_ACTIVE`/`DRAM_ACTIVE`) aligned to existing scrape interval. Assemble evidence pack (signal window + ctx). **Local read-only API** (unix socket) for the cli (D-0010 Endpoint 1). Source-tag every signal (TASK-0018). | Connect to real Prom/DCGM (+mock) → per-job MFU + wasted-$ context in evidence pack; cli renders local single-node view; **standalone-useful, no control-plane edge** (TASK-0022). |
| **M3** 确定性 RCA + abstain + lab | dmesg/XID (`/dev/kmsg` tail) + NCCL + nvidia-smi/NVML local collectors (fault-signal gap). Run rca **≥2-independent-signal gate (by source)** → **local Verdict (ABSTAIN \| class)**. Wire into lab fault injection. | Injected XID79/ECC/NVLink/NCCL-timeout → agent produces correct local fire/abstain; independence enforced by source, not declared field. |
| **M4** benchmark + scorecard + 闭源 gate | Expose evidence packs + local verdicts to the bench harness; deterministic, reproducible evidence-grounding; pin collection so packs are stable under the regression gate. | Agent output feeds `bench`; evidence-grounding reproducible; precision@fired / abstain regressions block on the gate. |
| **M5** 闭源控制面 MVP + egress | **Sole HTTPS egress** of the evidence pack to controlplane; **sole Verdict receiver**. **D-0011 capability executor:** validate declarative directive (rides in the controlplane response) against allowlist + **consent tiers** (Tier-0 default; Tier-1 `CAP_BPF`/`CAP_PERFMON` opt-in); **resource guards + kill-switch** (max duration/CPU/maps, auto-detach) on every collector; **outbound-only, zero inbound**; `Describe` handshake advertises catalog. | agent → controlplane → Verdict over HTTPS; directive validated + tier-gated; a probe can never stall a node; license enforced server-side (never in agent). |
| **M6** 设计伙伴验证 | Harden on real GPU fleets: validate cost attribution + fire-when-right on real evidence packs; validate profiling-cap (high-res burst frequency cap) and kill-switch under load; multi-round adaptive directive loop. | 1–2 partners run the DaemonSet read-only on real GPUs; cost attribution + fire/abstain confirmed; off-path guarantees hold under production load. |

## Invariants held every milestone
- Read-only & off-critical-path; never mutates a GPU; product runs with **no GPU** (mock).
- Evidence pack is the **only** payload to the controlplane; verdict decided server-side, only received here.
- Agent **always initiates** outbound HTTPS; **zero inbound** to the node.
- Controlplane sends **declarative directives only** — never code, playbooks, or bytecode (D-0011); eBPF/perf probes are read-only observers bundled in this open, auditable agent.
- cli reads the local API only; it **never uploads / never originates egress** (D-0010).
- No LLM narration in this repo; `proto/` read-only.
