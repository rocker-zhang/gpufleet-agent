//go:build gpu

package agent

import "testing"

// TestRealLogSource_GPU_TailsKmsg asserts the gpu-build real log source is the
// real /dev/kmsg tail, not a fixture (TASK-0056).
func TestRealLogSource_GPU_TailsKmsg(t *testing.T) {
	if _, ok := realLogSource("").(*kmsgLogSource); !ok {
		t.Fatalf("gpu realLogSource(\"\") = %T, want *kmsgLogSource", realLogSource(""))
	}
}

// TestCollectors_RealBranch_GPU_HasKmsgLogSource is the regression for TASK-0056:
// the REAL collector branch (`--collectors real`) MUST wire the real kmsg log
// source on the gpu build. The original bug hardcoded a FixtureLogSource on the
// real branch, so XID79/ECC-XID legs were never collected on real hardware and
// the gate could only ABSTAIN — which this test would have caught.
func TestCollectors_RealBranch_GPU_HasKmsgLogSource(t *testing.T) {
	rc := RuntimeConfig{
		Mode:            CollectorModeReal,
		DCGMExporterURL: "http://localhost:9400/metrics",
		Node:            "t",
	}
	cols, mode := rc.Collectors()
	if mode != CollectorModeReal {
		t.Fatalf("mode = %v, want real", mode)
	}
	var found bool
	for _, c := range cols {
		if le, ok := c.(LogEventCollector); ok {
			found = true
			if _, ok := le.Src.(*kmsgLogSource); !ok {
				t.Fatalf("real-branch LogEventCollector.Src = %T, want *kmsgLogSource", le.Src)
			}
		}
	}
	if !found {
		t.Fatal("real-branch Collectors() has no LogEventCollector")
	}
}
