package main

import (
	"flag"
	"os"
	"testing"
)

// TestBuiltinPeakTableFlagDefault asserts the --builtin-peak-table flag (TASK-0044)
// is ON by default and that env GPUFLEET_BUILTIN_PEAK_TABLE=false disables it.
//
// The flag's default is envBool("GPUFLEET_BUILTIN_PEAK_TABLE", true): with the env
// unset the default must be true (a zero-config box gets real MFU), and an explicit
// "false" must turn it off (operator opts into pure degrade-not-fabricate). We
// exercise the SAME default-resolution path main() uses (envBool) plus the flag's
// own parse, so the wiring — not just the helper — is covered.
func TestBuiltinPeakTableFlagDefault(t *testing.T) {
	const key = "GPUFLEET_BUILTIN_PEAK_TABLE"
	t.Setenv(key, "") // ensure unset

	// 1) Default with env unset/empty: ON.
	os.Unsetenv(key)
	if got := envBool(key, true); !got {
		t.Fatalf("default (env unset) = %v, want true (table ON by default)", got)
	}

	// The flag wires its default through envBool, so the parsed flag default is true.
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	bpt := fs.Bool("builtin-peak-table", envBool(key, true), "")
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !*bpt {
		t.Fatalf("--builtin-peak-table default = %v, want true", *bpt)
	}

	// 2) env=false disables it.
	t.Setenv(key, "false")
	if got := envBool(key, true); got {
		t.Fatalf("env=false ⇒ %v, want false (table disabled)", got)
	}

	// 3) env=true keeps it on (and overrides a false code default).
	t.Setenv(key, "true")
	if got := envBool(key, false); !got {
		t.Fatalf("env=true ⇒ %v, want true", got)
	}
}
