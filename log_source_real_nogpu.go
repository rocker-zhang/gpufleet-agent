//go:build !gpu

package agent

// realLogSource on the default (non-gpu) build has no /dev/kmsg access (the real
// kernel ring-buffer tail is isolated behind //go:build gpu). It returns a
// read-only NCCL file source when a path is given, else the in-memory fixture
// source — i.e. no real kmsg. This preserves the exact prior real-branch
// behavior so the default CI build / shipped binaries stay CPU-only and need no
// GPU (TASK-0056). The gpu build's counterpart tails real /dev/kmsg.
func realLogSource(ncclPath string) LogSource {
	if ncclPath != "" {
		return fileNCCLSource{path: ncclPath}
	}
	return FixtureLogSource{}
}
