//go:build gpu

package agent

// realLogSource is the gpu-build log source for the REAL collector branch
// (TASK-0056). It tails the real /dev/kmsg (kernel ring buffer → dmesg/XID
// events) and, when given, the NCCL log file — both strictly READ-ONLY
// (O_RDONLY; see kmsgLogSource). This is the seam that lets `--collectors real`
// collect the dmesg/XID corroboration leg on real hardware; without it the real
// branch read no kmsg, so XID79/ECC-XID signatures could never fire on a real
// box (only mock --inject-faults exercised them). The returned *kmsgLogSource
// implements killWirable, so the daemon's shutdown/kill-switch still interrupts
// an in-flight /dev/kmsg drain (TASK-0033) via LogEventCollector.SetKill.
func realLogSource(ncclPath string) LogSource {
	// Honor GPUFLEET_NCCL_LOG even when the path wasn't threaded through the flag,
	// so the real branch and the mock DefaultCollectors path agree on the NCCL
	// source (TASK-0056 carry: removes the env asymmetry between the two paths).
	if ncclPath == "" {
		ncclPath = defaultNCCLLog()
	}
	return newKmsgLogSource("/dev/kmsg", ncclPath)
}
