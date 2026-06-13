# gpufleet-agent

Apache-2.0 · OPEN module · `github.com/rocker-zhang/gpufleet-agent`

The **read-only collector / sidecar**. It sits off the critical path on top of a
customer's existing telemetry (DCGM-exporter / Prometheus / dmesg / NCCL), reads
it, and emits a **normalized evidence pack**. It never controls, orchestrates,
checkpoints, or otherwise touches the GPU, and is never in a job-execution path.

## No GPU required

The default build uses a **mock metrics source** (`mock.go`, `//go:build
!gpu`) — it runs anywhere, CPU-only, and is what CI and the shipped binaries
use. The real NVML-backed reader lives behind `//go:build gpu` (`nvml_gpu.go`)
and is compiled only in the dev lab. NVML usage there is strictly read-only
query — never device control.

```sh
go run ./cmd/agent --node my-host         # mock source, prints evidence JSON
go build -tags gpu ./cmd/agent            # lab-only NVML-backed build

# Point at REAL telemetry (read-only HTTP scrape, no NVML / no -tags gpu needed):
agent -serve --prometheus-url http://prometheus:9090 \
             --dcgm-exporter-url http://127.0.0.1:9400/metrics --node my-host
```

## Evidence is the only thing that leaves

The agent's sole outbound interaction with the closed control plane is one
HTTPS call carrying a **structured evidence pack** (a normalized signal window)
— never prompts, playbooks, or heuristics. The response is a `Verdict`. The
verdict logic (gate → playbook → LLM narration) runs entirely server-side.

## Boundaries

- Read-only & off-critical-path. No GPU control of any kind.
- `proto/` is a read-only dependency; this repo never edits it.
- The closed control plane is never bundled into this open build.

## Develop

```sh
go test -race ./...   # default build = mock NVML
go vet ./...
```

Releases are cut by GoReleaser on a `v*` tag: static cross builds for
amd64+arm64, cosign keyless (Sigstore OIDC) signatures, SLSA provenance, and an
SBOM. arm64 is a first-class CI + release target.
