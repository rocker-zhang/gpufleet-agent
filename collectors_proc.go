package agent

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the build-AGNOSTIC, read-only PROC/sysfs link-health collector
// (TASK-0053). It reads the PCIe link width/speed each device negotiated from
// sysfs and, when the CURRENT link is below the device's MAX, emits a pre-formed
// `link.degraded.width.<token>`@PROC timeline leg. That leg is the INDEPENDENT,
// non-DCGM corroborator the LINK_DEGRADED gate needs alongside the DCGM
// `link.error.<uuid>` counter leg — two DISTINCT sources, so the >=2-source gate
// can FIRE on genuinely-collected telemetry.
//
// HONESTY (RULES §B): a leg is emitted ONLY when sysfs genuinely reports a
// current width/speed STRICTLY below the negotiated max. Equal width/speed ⇒ no
// leg; missing/unreadable files ⇒ degrade (no leg), never fabricate. It NEVER
// writes back and dials NO network endpoint — it only reads files under a sysfs
// root (RULES §A). Source() is SIGNAL_SOURCE_PROC.
//
// It is unit-testable WITHOUT a GPU or real sysfs: SysfsRoot points at a temp dir
// laid out like /sys/bus/pci/devices/<addr>/{current,max}_link_{width,speed}.
// File reads need no GPU, so the whole collector compiles + tests on the default
// !gpu build.

// defaultSysfsPCIRoot is the standard sysfs PCI device root. It is overridable
// (SysfsRoot) so tests point it at a temp dir.
const defaultSysfsPCIRoot = "/sys/bus/pci/devices"

// procReadCap bounds a single sysfs attribute read so a pathological/huge file
// can never stall or balloon the collector (read-only off-path bound, RULES §A).
// sysfs link attributes are a handful of bytes; this is a generous safety cap.
const procReadCap = 4 << 10 // 4 KiB

// ProcLinkCollector reads PCIe link width/speed from sysfs and emits a
// `link.degraded.width.<token>`@PROC leg per device whose CURRENT link is below
// its MAX. It is read-only and side-effect-free (RULES §A).
type ProcLinkCollector struct {
	// SysfsRoot is the sysfs PCI device root to scan. Empty ⇒ defaultSysfsPCIRoot.
	// In tests this is a temp dir laid out like the real sysfs tree.
	SysfsRoot string
	// Node, when set, stamps the node onto the observation provenance.
	Node string
}

func (c ProcLinkCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC
}

func (c ProcLinkCollector) root() string {
	if c.SysfsRoot != "" {
		return c.SysfsRoot
	}
	return defaultSysfsPCIRoot
}

// Collect scans the sysfs PCI device root and emits one degraded-link leg per
// device whose current PCIe link width OR speed is strictly below its negotiated
// max. A missing root, unreadable entries, or non-degraded links yield no leg
// (degrade, never fabricate). It is pure file reads: no GPU, no cgo, no network.
func (c ProcLinkCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	root := c.root()
	obs := Observation{
		Source: c.Source(),
		Provenance: map[string]string{
			"source":     "proc-sysfs-pcie-link",
			"sysfs_root": root,
		},
	}
	if c.Node != "" {
		obs.Provenance["node"] = c.Node
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		// No sysfs / unreadable root ⇒ degrade cleanly: no leg, never fatal.
		obs.Provenance["degraded"] = "sysfs root unreadable"
		return obs, nil
	}

	// Deterministic device ordering by PCI address.
	addrs := make([]string, 0, len(entries))
	for _, e := range entries {
		addrs = append(addrs, e.Name())
	}
	sort.Strings(addrs)

	for _, addr := range addrs {
		dir := filepath.Join(root, addr)
		// PCIe link WIDTH downgrade (current < max). Both must parse for a verdict;
		// a missing/garbage attribute degrades (no leg) rather than fabricating.
		if curW, ok1 := readLinkWidth(filepath.Join(dir, "current_link_width")); ok1 {
			if maxW, ok2 := readLinkWidth(filepath.Join(dir, "max_link_width")); ok2 {
				if maxW > 0 && curW > 0 && curW < maxW {
					obs.Timeline = append(obs.Timeline, &gpufleetv1.TimelineEntry{
						Ts:       timestamppb.New(now),
						Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC,
						SignalId: "link.degraded.width." + sanitizeToken(addr),
						Label: "PCIe link width downgraded (x" +
							strconv.Itoa(curW) + " < x" + strconv.Itoa(maxW) + ") on " + addr,
					})
					continue // one leg per device; width downgrade already cited.
				}
			}
		}
		// PCIe link SPEED downgrade (current < max), e.g. "2.5 GT/s" < "16.0 GT/s".
		if curS, ok1 := readLinkSpeed(filepath.Join(dir, "current_link_speed")); ok1 {
			if maxS, ok2 := readLinkSpeed(filepath.Join(dir, "max_link_speed")); ok2 {
				if maxS > 0 && curS > 0 && curS < maxS {
					obs.Timeline = append(obs.Timeline, &gpufleetv1.TimelineEntry{
						Ts:       timestamppb.New(now),
						Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC,
						SignalId: "link.degraded.speed." + sanitizeToken(addr),
						Label:    "PCIe link speed downgraded on " + addr,
					})
				}
			}
		}
	}
	return obs, nil
}

// readLinkWidth reads a sysfs *_link_width attribute (a plain integer lane count,
// e.g. "16"). Missing/unparseable ⇒ (0, false) so the caller degrades.
func readLinkWidth(path string) (int, bool) {
	s, ok := readSysfsAttr(path)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// readLinkSpeed reads a sysfs *_link_speed attribute (e.g. "16.0 GT/s PCIe" or
// "2.5 GT/s") and returns the leading GT/s number scaled to an integer
// (tenths-of-GT/s) so a pure-integer comparison orders speeds correctly without
// float fuzz. Missing/unparseable ⇒ (0, false) so the caller degrades.
func readLinkSpeed(path string) (int, bool) {
	s, ok := readSysfsAttr(path)
	if !ok {
		return 0, false
	}
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) == 0 {
		return 0, false
	}
	f, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || f <= 0 {
		return 0, false
	}
	return int(f*10 + 0.5), true // tenths of GT/s, rounded
}

// readSysfsAttr reads a single sysfs attribute file read-only, bounded by
// procReadCap. A missing/unreadable file ⇒ ("", false) so the caller degrades.
// It NEVER writes back (RULES §A).
func readSysfsAttr(path string) (string, bool) {
	f, err := os.Open(path) // O_RDONLY: read-only, never writes
	if err != nil {
		return "", false
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, procReadCap)
	n, _ := f.Read(buf)
	if n <= 0 {
		return "", false
	}
	return string(buf[:n]), true
}
