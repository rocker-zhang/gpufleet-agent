//go:build !gpu

package agent

import "testing"

// TestRealLogSource_NoGPU asserts the default (non-gpu) build has no /dev/kmsg:
// a fixture source by default, a file NCCL source when a path is given
// (preserving the prior real-branch behavior, TASK-0056).
func TestRealLogSource_NoGPU(t *testing.T) {
	if _, ok := realLogSource("").(FixtureLogSource); !ok {
		t.Fatalf("no-gpu realLogSource(\"\") = %T, want FixtureLogSource", realLogSource(""))
	}
	if _, ok := realLogSource("/tmp/nccl.log").(fileNCCLSource); !ok {
		t.Fatalf("no-gpu realLogSource(path) = %T, want fileNCCLSource", realLogSource("/tmp/nccl.log"))
	}
}
