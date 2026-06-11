//go:build !gpu

package agent

import (
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the DEFAULT (no-GPU) build's signal sources. It provides three
// deterministic, replayable mock collectors — mock-DCGM, mock-Prometheus, and
// mock-dmesg/XID — that emit fixed sample shapes mirroring what the real
// read-only sources would produce. NO real scrape load, no GPU, no cgo: these
// stand in for the M2-lab real sources (TASK-0023) so the data-plane spine is
// exercised GPU-less on amd64 + arm64.
//
// The fixtures deliberately include:
//   - a HEALTHY device (GPU-mock-0001): high MFU ⇒ cost wedge wasted = $0.
//   - an IDLE device   (GPU-mock-0002): near-zero MFU ⇒ wasted > $0.
//   - a device whose PEAK is missing from one source (GPU-mock-0003): proves
//     missing-field degradation (MFU cannot be computed ⇒ marked, not faked).

const (
	mockNode = "mock-node"

	// dcgmFIProfPipeTensorActive is the public DCGM profiling field id/symbol the
	// real DCGM-exporter exposes; carried raw, semantics derived downstream.
	dcgmFIProfPipeTensorActive = 1003
	dcgmSymPipeTensorActive    = "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE"
)

// MockDCGMCollector emits DCGM-exporter-shaped device measurements: per-device
// achieved-FLOP/peak and tensor-active samples. It does NOT supply device->job
// mappings (DCGM has no scheduler view) — that is the Prometheus/scheduler
// source's job, and the normalizer marks the gap if no source supplies it.
type MockDCGMCollector struct{ Node string }

func (c MockDCGMCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM
}

func (c MockDCGMCollector) node() string {
	if c.Node != "" {
		return c.Node
	}
	return mockNode
}

func (c MockDCGMCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	ws := window.Seconds()
	node := c.node()

	// Raw tensor-active series, two samples per device (start + end of window).
	series := func(uuid string, frac float64) *gpufleetv1.DcgmFieldSeries {
		return &gpufleetv1.DcgmFieldSeries{
			DeviceUuid:  uuid,
			FieldSymbol: dcgmSymPipeTensorActive,
			FieldId:     dcgmFIProfPipeTensorActive,
			Unit:        "ratio",
			Samples: []*gpufleetv1.Sample{
				{Ts: timestamppb.New(now.Add(-window)), Value: frac},
				{Ts: timestamppb.New(now), Value: frac},
			},
		}
	}

	return Observation{
		Source: c.Source(),
		DcgmSeries: []*gpufleetv1.DcgmFieldSeries{
			series("GPU-mock-0001", 0.70),
			series("GPU-mock-0002", 0.05),
			series("GPU-mock-0003", 0.40),
		},
		DeviceWindows: []DeviceWindow{
			{ // HEALTHY: ~70% MFU
				UUID: "GPU-mock-0001", Node: node, Model: "A10", WindowSeconds: ws,
				AchievedFLOPs: 0.70 * peakA10FLOPS * ws, AchievedFLOPsKnown: true,
				TensorActiveSecs: 0.70 * ws, TensorActiveKnown: true,
				PeakFLOPS: peakA10FLOPS, PeakFLOPSKnown: true,
				// DCGM has no billing rate; cost is left unknown here and filled by
				// the Prometheus/scheduler source on merge.
				CostKnown: false,
			},
			{ // IDLE: ~5% MFU
				UUID: "GPU-mock-0002", Node: node, Model: "A10", WindowSeconds: ws,
				AchievedFLOPs: 0.05 * peakA10FLOPS * ws, AchievedFLOPsKnown: true,
				TensorActiveSecs: 0.05 * ws, TensorActiveKnown: true,
				PeakFLOPS: peakA10FLOPS, PeakFLOPSKnown: true,
				CostKnown: false,
			},
			{ // PEAK MISSING from DCGM: MFU cannot be computed from this source.
				UUID: "GPU-mock-0003", Node: node, Model: "A10", WindowSeconds: ws,
				AchievedFLOPs: 0.40 * peakA10FLOPS * ws, AchievedFLOPsKnown: true,
				TensorActiveSecs: 0.40 * ws, TensorActiveKnown: true,
				PeakFLOPSKnown: false, // <-- degraded: no peak from DCGM
				CostKnown:      false,
			},
		},
		Provenance: map[string]string{
			"source":         "mock-dcgm",
			"exporter":       "dcgm-exporter-mock/3.3.0",
			"scrape_seconds": "15",
		},
	}, nil
}

// peakA10FLOPS is the advertised BF16 tensor-core peak for the mock A10 fixture
// (FLOP/s). A real source reads this from the device spec; here it is a fixed,
// public-shaped constant so MFU math is deterministic.
const peakA10FLOPS = 1.25e14

// MockPrometheusCollector emits a Prometheus/PromQL-shaped view: the
// device->job ownership mapping and the per-device billing rate (the scheduler
// + cost-rate context that DCGM lacks). It supplies no fault events.
type MockPrometheusCollector struct{ Node string }

func (c MockPrometheusCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS
}

func (c MockPrometheusCollector) node() string {
	if c.Node != "" {
		return c.Node
	}
	return mockNode
}

func (c MockPrometheusCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	node := c.node()
	const costPerHour = 1.10 // $/hr for the mock A10

	mapping := func(uuid, job string, idx uint32) *gpufleetv1.DeviceJobMapping {
		return &gpufleetv1.DeviceJobMapping{
			DeviceUuid:         uuid,
			DeviceIndex:        idx,
			Node:               node,
			JobId:              job,
			PeakTflops:         peakA10FLOPS / 1e12,
			CostRateUsdPerHour: costPerHour,
		}
	}

	// Cost + peak the Prometheus/scheduler side knows about. GPU-mock-0003's
	// peak is supplied HERE (so the merged window can still compute MFU even
	// though DCGM omitted it) — proving cross-source field completion.
	costWin := func(uuid string) DeviceWindow {
		return DeviceWindow{
			UUID: uuid, Node: node, Model: "A10",
			WindowSeconds:  window.Seconds(),
			PeakFLOPS:      peakA10FLOPS,
			PeakFLOPSKnown: true,
			CostPerHour:    costPerHour,
			CostKnown:      true,
		}
	}

	return Observation{
		Source: c.Source(),
		Mappings: []*gpufleetv1.DeviceJobMapping{
			mapping("GPU-mock-0001", "job-train-a", 0),
			mapping("GPU-mock-0002", "job-idle-b", 1),
			mapping("GPU-mock-0003", "job-train-a", 2),
		},
		DeviceWindows: []DeviceWindow{
			costWin("GPU-mock-0001"),
			costWin("GPU-mock-0002"),
			costWin("GPU-mock-0003"),
		},
		Provenance: map[string]string{
			"source":   "mock-prometheus",
			"promql":   "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE / on(uuid) job:gpu_owner",
			"endpoint": "http://localhost:9090 (mock)",
		},
	}, nil
}

// MockDmesgCollector emits dmesg/XID-shaped discrete fault events. The default
// fixture is a HEALTHY node: zero XID events (so the cost story stands on its
// own, fault-free). It supplies no mappings and no measurements.
type MockDmesgCollector struct {
	Node string
	// Inject, when set, adds deterministic XID events for tests/lab replay. The
	// default (nil) is a clean, healthy node.
	Inject []*gpufleetv1.XidEvent
}

func (c MockDmesgCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DMESG_XID
}

func (c MockDmesgCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	return Observation{
		Source:     c.Source(),
		XidEvents:  c.Inject,
		Provenance: map[string]string{"source": "mock-dmesg", "tail": "/dev/kmsg (mock)"},
	}, nil
}

// DefaultCollectors returns the default (no-GPU) signal sources wired for the
// agent: mock-DCGM + mock-Prometheus + mock-dmesg/XID. With `-tags gpu` the
// gpu-tagged build supplies NVML-backed read-only collectors instead.
func DefaultCollectors(node string) []Collector {
	return []Collector{
		MockDCGMCollector{Node: node},
		MockPrometheusCollector{Node: node},
		MockDmesgCollector{Node: node},
	}
}
