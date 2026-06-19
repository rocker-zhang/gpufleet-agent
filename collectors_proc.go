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
// sysfs and emits two kinds of pre-formed @PROC timeline leg:
//
//   - `device.lost.<addr>`     — an NVIDIA GPU whose negotiated link is fully DOWN
//     (current_link_width == 0 with a known max>0): the GPU has fallen off the
//     PCIe bus. This is the INDEPENDENT, non-dmesg corroborator the XID79 gate
//     needs alongside the kernel `dmesg.xid79`@DMESG_XID leg.
//   - `link.degraded.{width,speed}.<addr>` — the CURRENT link is below the device's
//     MAX (but still up): the INDEPENDENT, non-DCGM corroborator the LINK_DEGRADED
//     gate needs alongside the DCGM `link.error.<uuid>` counter leg.
//
// Each is two DISTINCT sources from its DCGM/dmesg counterpart, so the >=2-source
// gate can FIRE on genuinely-collected telemetry.
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
		// GPU FALLEN OFF THE BUS (device lost), the INDEPENDENT non-dmesg leg of the
		// XID79 gate. When an NVIDIA GPU drops off the PCIe bus the kernel reports its
		// negotiated link as fully DOWN: `current_link_width` reads 0 while the device
		// still advertises a non-zero `max_link_width`. That width-0 (no link at all)
		// is a CONCRETE, genuinely-observed "device unreachable on the bus" fact —
		// categorically distinct from a width DOWNGRADE (current>0 but < max, handled
		// below). HONESTY (RULES §B): we emit `device.lost.<addr>`@PROC ONLY when the
		// device's `vendor` is NVIDIA (0x10de) AND current width is 0 with a known
		// max>0, so a non-GPU device, a powered-down (D3) slot we cannot attribute, or
		// any unreadable attribute degrades (no leg) rather than fabricating a lost
		// GPU. This is the same provenance-guarded Observation.Timeline channel the
		// link-degraded legs use; Source=PROC. It is the XID79 corroborator that lets
		// the gate FIRE on two genuine distinct sources (dmesg.xid79@DMESG_XID +
		// device.lost@PROC).
		//
		// PRECISION GUARDRAIL (do not weaken): current_link_width can also read 0 for a
		// perfectly healthy GPU in runtime power management (D3cold / ASPM suspend), so
		// this leg is NOT proof-of-fault on its own. It MUST only ever be consumed as a
		// CORROBORATOR alongside a genuine kernel dmesg.xid79@DMESG_XID leg (which a
		// healthy suspended GPU never emits; a GPU under load is in D0, not suspended).
		// Never promote device.lost@PROC to a standalone-actionable signal without a
		// stronger basis (e.g. a seen-then-gone transition) — false-positive vector.
		if isNVIDIADevice(dir) {
			if curW, ok1 := readLinkWidth(filepath.Join(dir, "current_link_width")); ok1 {
				if maxW, ok2 := readLinkWidth(filepath.Join(dir, "max_link_width")); ok2 {
					if maxW > 0 && curW == 0 {
						obs.Timeline = append(obs.Timeline, &gpufleetv1.TimelineEntry{
							Ts:       timestamppb.New(now),
							Source:   gpufleetv1.SignalSource_SIGNAL_SOURCE_PROC,
							SignalId: "device.lost." + sanitizeToken(addr),
							Label: "NVIDIA GPU fallen off the PCIe bus (link down, x0 of x" +
								strconv.Itoa(maxW) + ") at " + addr,
						})
						continue // device lost dominates any link-degraded reading.
					}
				}
			}
		}
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

// nvidiaPCIVendorID is the PUBLIC PCI vendor id for NVIDIA, as sysfs reports it
// in each device's `vendor` attribute. PUBLIC fact only (RULES §F).
const nvidiaPCIVendorID = "0x10de"

// isNVIDIADevice reports whether the sysfs PCI device dir belongs to NVIDIA, by
// reading its `vendor` attribute and comparing to nvidiaPCIVendorID. A
// missing/unreadable/non-matching vendor ⇒ false, so the device.lost leg is only
// ever attributed to a genuine NVIDIA GPU slot (degrade, never fabricate). Pure
// read-only file read.
func isNVIDIADevice(dir string) bool {
	v, ok := readSysfsAttr(filepath.Join(dir, "vendor"))
	if !ok {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(v), nvidiaPCIVendorID)
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
