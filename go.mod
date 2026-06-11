module github.com/rocker-zhang/gpufleet-agent

go 1.26.0

require (
	github.com/rocker-zhang/gpufleet-proto/gen/go v0.1.0
	github.com/rocker-zhang/gpufleet-semantics v0.0.0
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/grpc v1.81.1 // indirect
)

// Poly-repo: in CI each dependency is consumed at a pinned tag (the proto gen
// module at proto/v0.1.0). For local workspace builds these replace directives
// point at the sibling repos so the build resolves offline against the vendored
// real gen types — NOT a hand-rolled mirror.
replace github.com/rocker-zhang/gpufleet-proto/gen/go => ../proto/gen/go

replace github.com/rocker-zhang/gpufleet-semantics => ../semantics
