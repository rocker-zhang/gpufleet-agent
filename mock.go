//go:build !gpu

package agent

// MockReader is the default (no-GPU) metrics source. It emits a fixed,
// deterministic evidence window that mirrors the shape a real DCGM/Prometheus
// reader would produce. This is what the standard build and CI use.
type MockReader struct {
	NodeName string
}

// DefaultReader returns the build's default reader. Without the `gpu` build
// tag this is the mock; with `-tags gpu` it is the NVML-backed reader.
func DefaultReader(node string) Reader { return MockReader{NodeName: node} }

// Read implements Reader with deterministic synthetic data.
func (m MockReader) Read() (Evidence, error) {
	node := m.NodeName
	if node == "" {
		node = "mock-node"
	}
	return Evidence{
		Source: "mock",
		Devices: []DeviceMetrics{
			{
				UUID: "GPU-mock-0001", Node: node, Model: "A10",
				WindowSeconds: 60, AchievedFLOPs: 6e14, TensorActiveSecs: 42,
				ECCDoubleBitErrs: 0, XIDs: nil,
			},
			{
				UUID: "GPU-mock-0002", Node: node, Model: "A10",
				WindowSeconds: 60, AchievedFLOPs: 3e14, TensorActiveSecs: 20,
				ECCDoubleBitErrs: 0, XIDs: nil,
			},
		},
	}, nil
}
