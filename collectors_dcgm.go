package agent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file is the DEFAULT (no-GPU) build's METRICS-FALLBACK collector: a local
// DCGM-exporter /metrics scrape reader. It is used ONLY when Prometheus is
// absent, a field is missing, or higher resolution is needed (D-0009). It parses
// the Prometheus text-exposition format DCGM-exporter already serves — it does
// NOT add a second exporter; it reads the one that is already running. It is a
// pure HTTP GET, read-only, off the GPU critical path (RULES §A).
//
// PROFILING CAP (off-critical-path linchpin, D-0009): the DCGM *profiling*
// fields (SM_ACTIVE, PIPE_TENSOR_ACTIVE, DRAM_ACTIVE) default to the customer's
// existing scrape interval. A high-resolution burst is OPT-IN and frequency-
// capped by a rate gate (RateCappedScraper) so the agent can never drive the
// exporter faster than the configured cap. TestProfilingBurstRateCapped asserts
// the cap actually limits frequency.
//
// It is testable WITHOUT real hardware: point ScrapeURL at an httptest server
// serving a fixture DCGM-exporter text payload (see testdata), or pass a
// payload directly to parseDCGMExposition.

// Public DCGM profiling field symbols (the "profiling" group that is rate-capped
// because it is the most scrape-expensive). Only public symbols — no proprietary
// or closed semantics (RULES §F).
const (
	dcgmSymSMActive   = "DCGM_FI_PROF_SM_ACTIVE"
	dcgmSymDRAMActive = "DCGM_FI_PROF_DRAM_ACTIVE"

	// dcgmSymPipeTensorActive / dcgmFIProfPipeTensorActive are the public DCGM
	// profiling field symbol/id the real DCGM-exporter exposes; carried raw,
	// semantics derived downstream. Build-agnostic so both the mock (!gpu) and the
	// real DCGM collector reference one definition.
	dcgmSymPipeTensorActive    = "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE"
	dcgmFIProfPipeTensorActive = 1003

	// dcgmSymECCDBETotal is the public DCGM field for the lifetime count of
	// volatile UNCORRECTABLE (double-bit) ECC errors. It is the DCGM leg of the
	// ECC-uncorrectable gate signature: a delta>0 over the window, corroborated by
	// an INDEPENDENT dmesg ECC Xid, fires FAULT_CLASS_ECC_UNCORRECTABLE. PUBLIC
	// field only (RULES §F). Read-only counter; the agent reports the delta, never
	// resets it.
	dcgmSymECCDBETotal = "DCGM_FI_DEV_ECC_DBE_VOL_TOTAL"

	// The link-error counters are the public DCGM fields for NVLink/PCIe
	// interconnect errors. They are the DCGM leg of the LINK_DEGRADED gate
	// signature: a cumulative-counter delta>0 over the window, corroborated by an
	// INDEPENDENT (non-DCGM) link width/speed downgrade, fires
	// FAULT_CLASS_LINK_DEGRADED. PUBLIC fields only (RULES §F). Read-only counters;
	// the agent reports the delta, never resets them. Each is summed across the
	// link-error counters into a single per-device delta (any link error this
	// window is enough to be the DCGM leg); the leg is `link.error.<uuid>`@DCGM.
	dcgmSymNVLinkCRCFlitErr = "DCGM_FI_DEV_NVLINK_CRC_FLIT_ERROR_COUNT_TOTAL"
	dcgmSymNVLinkReplayErr  = "DCGM_FI_DEV_NVLINK_REPLAY_ERROR_COUNT_TOTAL"
	dcgmSymPCIeReplay       = "DCGM_FI_DEV_PCIE_REPLAY_COUNTER"
)

// dcgmLinkErrSymbols is the set of cumulative link-error counters whose summed
// per-device value is delta'd into the LINK_DEGRADED DCGM leg. They are PUBLIC
// DCGM field symbols only (RULES §F).
var dcgmLinkErrSymbols = map[string]bool{
	dcgmSymNVLinkCRCFlitErr: true,
	dcgmSymNVLinkReplayErr:  true,
	dcgmSymPCIeReplay:       true,
}

// dcgmProfilingSymbols is the set of profiling fields subject to the burst cap.
var dcgmProfilingSymbols = map[string]bool{
	dcgmSymSMActive:         true,
	dcgmSymDRAMActive:       true,
	dcgmSymPipeTensorActive: true,
}

// dcgmFieldIDs maps the public symbols this collector recognizes to their public
// numeric DCGM field ids (carried raw so downstream needs no DCGM headers).
var dcgmFieldIDs = map[string]uint32{
	dcgmSymPipeTensorActive: dcgmFIProfPipeTensorActive,
	dcgmSymSMActive:         1002,
	dcgmSymDRAMActive:       1005,
}

// DCGMExporterCollector reads the local DCGM-exporter /metrics endpoint and
// parses the text exposition into raw DcgmFieldSeries + per-device windows. It
// is the FALLBACK metrics source. It implements the read-only Collector.
type DCGMExporterCollector struct {
	// ScrapeURL is the DCGM-exporter metrics endpoint (e.g.
	// "http://localhost:9400/metrics"). In tests this is an httptest URL.
	ScrapeURL string
	// Node, when set, stamps the node onto observations (per-node DaemonSet).
	Node string
	// HTTPClient is the client used to scrape. Nil uses a short-timeout default.
	HTTPClient *http.Client
	// Timeout bounds the scrape. Zero uses 5s.
	Timeout time.Duration

	// ProfilingBurst opts into high-resolution profiling scrapes. When false
	// (default), profiling fields are still parsed but the collector does not
	// scrape faster than the customer's existing interval — the daemon drives it
	// at its normal cadence and the cap is irrelevant. When true, callers may
	// drive Collect rapidly, but ScrapeAllowed() gates the actual HTTP scrape to
	// BurstCap frequency.
	ProfilingBurst bool
	// BurstCap is the minimum interval between profiling-burst scrapes. Zero uses
	// DefaultProfilingBurstInterval. The rate gate guarantees the agent never
	// scrapes the exporter more often than this — the off-critical-path cap.
	BurstCap time.Duration

	// now overrides the clock for deterministic cap tests. Nil uses time.Now.
	now func() time.Time

	mu          sync.Mutex
	lastScrapeT time.Time
	// prevDBE holds the last-seen cumulative uncorrectable-ECC (double-bit) counter
	// per device UUID, so Collect can report the DELTA over the window rather than
	// the lifetime total. ECC DBE is a cumulative counter: a delta requires two
	// readings, so the FIRST scrape establishes the baseline and reports no delta
	// (honest — a single reading is not evidence of a new error this window).
	prevDBE map[string]uint64
	// prevLinkErr holds the last-seen SUM of the cumulative link-error counters
	// (NVLink CRC/replay + PCIe replay) per device UUID, so Collect reports the
	// per-WINDOW delta rather than the lifetime total — mirroring prevDBE exactly,
	// including the first-scrape baseline (a single cumulative reading is not, by
	// itself, evidence of a new link error this window).
	prevLinkErr map[string]uint64
}

// DefaultProfilingBurstInterval is the default floor between profiling-burst
// scrapes when burst is opted into without an explicit BurstCap.
const DefaultProfilingBurstInterval = time.Second

func (c *DCGMExporterCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM
}

func (c *DCGMExporterCollector) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *DCGMExporterCollector) burstCap() time.Duration {
	if c.BurstCap > 0 {
		return c.BurstCap
	}
	return DefaultProfilingBurstInterval
}

// ScrapeAllowed reports whether a profiling-burst scrape is permitted now under
// the frequency cap, and records the decision time when it returns true. When
// ProfilingBurst is false it always returns true (the daemon's own interval is
// the rate limiter; there is no burst to cap). This is the rate gate the
// off-critical-path profiling cap relies on; TestProfilingBurstRateCapped
// asserts it limits frequency.
func (c *DCGMExporterCollector) ScrapeAllowed() bool {
	if !c.ProfilingBurst {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if !c.lastScrapeT.IsZero() && now.Sub(c.lastScrapeT) < c.burstCap() {
		return false // capped: too soon since the last burst scrape
	}
	c.lastScrapeT = now
	return true
}

func (c *DCGMExporterCollector) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	to := c.Timeout
	if to <= 0 {
		to = 5 * time.Second
	}
	return &http.Client{Timeout: to}
}

// Collect scrapes the local DCGM-exporter and parses its text exposition. When
// a profiling-burst scrape is capped (too soon), it returns ErrProfilingCapped
// so the daemon counts+degrades for this tick rather than exceeding the cap —
// the agent never adds scrape load beyond the configured cap.
func (c *DCGMExporterCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	if c.ScrapeURL == "" {
		return Observation{}, fmt.Errorf("agent: DCGMExporterCollector requires ScrapeURL")
	}
	if !c.ScrapeAllowed() {
		return Observation{}, ErrProfilingCapped
	}
	to := c.Timeout
	if to <= 0 {
		to = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.ScrapeURL, nil)
	if err != nil {
		return Observation{}, err
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return Observation{}, fmt.Errorf("agent: dcgm-exporter scrape: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Observation{}, fmt.Errorf("agent: dcgm-exporter scrape status %d", resp.StatusCode)
	}
	obs, err := parseDCGMExposition(resp.Body, now, window, c.nodeOrDefault())
	if err != nil {
		return Observation{}, err
	}
	c.applyDBEDelta(obs.DeviceWindows)
	c.applyLinkErrDelta(obs.DeviceWindows)
	return obs, nil
}

// applyDBEDelta converts each device's cumulative uncorrectable-ECC counter (as
// parsed) into the per-WINDOW delta against the last scrape, updating the stored
// baseline. ECC DBE is a monotonic counter, so the delta (not the lifetime total)
// is the evidence of a NEW error this window. The first reading establishes the
// baseline and reports a zero delta — a single cumulative reading is not, by
// itself, evidence of a new error (honesty, RULES §B). A counter reset (current <
// prior, e.g. driver reload) is clamped to zero rather than reported as a huge
// spurious delta.
func (c *DCGMExporterCollector) applyDBEDelta(dws []DeviceWindow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prevDBE == nil {
		c.prevDBE = map[string]uint64{}
	}
	for i := range dws {
		dw := &dws[i]
		if !dw.ECCDoubleBitKnown {
			continue
		}
		cur := dw.ECCDoubleBitErrs
		prev, seen := c.prevDBE[dw.UUID]
		c.prevDBE[dw.UUID] = cur
		switch {
		case !seen || cur < prev:
			dw.ECCDoubleBitErrs = 0 // baseline / counter reset ⇒ no delta this window.
		default:
			dw.ECCDoubleBitErrs = cur - prev
		}
	}
}

// applyLinkErrDelta converts each device's cumulative link-error counter SUM (as
// parsed) into the per-WINDOW delta against the last scrape, updating the stored
// baseline. It mirrors applyDBEDelta EXACTLY: the link-error counters are
// monotonic, so the delta (not the lifetime total) is the evidence of a NEW link
// error this window. The first reading establishes the baseline and reports a
// zero delta (a single cumulative reading is not, by itself, evidence of a new
// error — honesty, RULES §B). A counter reset (current < prior, e.g. driver
// reload) is clamped to zero rather than reported as a spurious spike.
func (c *DCGMExporterCollector) applyLinkErrDelta(dws []DeviceWindow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.prevLinkErr == nil {
		c.prevLinkErr = map[string]uint64{}
	}
	for i := range dws {
		dw := &dws[i]
		if !dw.LinkErrorsKnown {
			continue
		}
		cur := dw.LinkErrors
		prev, seen := c.prevLinkErr[dw.UUID]
		c.prevLinkErr[dw.UUID] = cur
		switch {
		case !seen || cur < prev:
			dw.LinkErrors = 0 // baseline / counter reset ⇒ no delta this window.
		default:
			dw.LinkErrors = cur - prev
		}
	}
}

func (c *DCGMExporterCollector) nodeOrDefault() string {
	if c.Node != "" {
		return c.Node
	}
	return ""
}

// ErrProfilingCapped signals that a profiling-burst scrape was skipped because
// the frequency cap has not elapsed. It is a degrade signal, never a node fault.
var ErrProfilingCapped = fmt.Errorf("agent: dcgm profiling burst rate-capped")

// dcgmParsedSample is one parsed exposition line: metric + labels + value.
type dcgmParsedSample struct {
	symbol string
	labels map[string]string
	value  float64
}

// parseDCGMExposition parses the Prometheus text-exposition format served by
// DCGM-exporter into a source-tagged Observation: raw DcgmFieldSeries (for the
// profiling fields) plus per-device windows derived from the parsed ratios.
// Fields the payload did not contain are simply absent → the normalizer
// degrades. It is pure parsing: no GPU, no cgo, no scrape side effects.
func parseDCGMExposition(r io.Reader, now time.Time, window time.Duration, nodeFallback string) (Observation, error) {
	ws := window.Seconds()
	samples, err := scanExposition(r)
	if err != nil {
		return Observation{}, err
	}

	// Accumulate per device. We track tensor-active (for the cost wedge tensor
	// signal), and SM_ACTIVE as the utilization ratio that drives achieved-FLOPs
	// when a peak is known elsewhere. DCGM-exporter has no $/hour or peak spec, so
	// cost/peak are intentionally left unknown here (degrade; Prometheus or the
	// device spec completes them on merge).
	type acc struct {
		dw          DeviceWindow
		tensorRatio float64
		tensorKnown bool
	}
	devs := map[string]*acc{}
	series := map[string]*gpufleetv1.DcgmFieldSeries{} // key: uuid|symbol

	get := func(uuid, node, model string) *acc {
		a, ok := devs[uuid]
		if !ok {
			a = &acc{dw: DeviceWindow{UUID: uuid, WindowSeconds: ws}}
			devs[uuid] = a
		}
		if node != "" && a.dw.Node == "" {
			a.dw.Node = node
		}
		if model != "" && a.dw.Model == "" {
			a.dw.Model = model
		}
		return a
	}

	for _, s := range samples {
		uuid := s.labels["UUID"]
		if uuid == "" {
			uuid = s.labels["gpu_uuid"]
		}
		if uuid == "" {
			continue // device-less line; cannot attribute
		}
		node := s.labels["Hostname"]
		if node == "" {
			node = nodeFallback
		}
		model := s.labels["modelName"]
		a := get(uuid, node, model)

		// Carry the profiling fields raw as DcgmFieldSeries (provenance-preserving).
		if dcgmProfilingSymbols[s.symbol] {
			key := uuid + "|" + s.symbol
			fs, ok := series[key]
			if !ok {
				fs = &gpufleetv1.DcgmFieldSeries{
					DeviceUuid:  uuid,
					FieldSymbol: s.symbol,
					FieldId:     dcgmFieldIDs[s.symbol],
					Unit:        "ratio",
				}
				series[key] = fs
			}
			fs.Samples = append(fs.Samples, &gpufleetv1.Sample{
				Ts: timestamppb.New(now), Value: s.value,
			})
		}

		switch s.symbol {
		case dcgmSymPipeTensorActive:
			a.tensorRatio, a.tensorKnown = s.value, true
			a.dw.TensorActiveSecs, a.dw.TensorActiveKnown = s.value*ws, true
		case dcgmSymSMActive:
			// SM_ACTIVE is a utilization ratio; without a known peak we cannot
			// turn it into absolute FLOPs, so we do NOT fabricate AchievedFLOPs
			// here. It is carried raw (series) for downstream corroboration.
		case dcgmSymECCDBETotal:
			// Cumulative uncorrectable (double-bit) ECC counter. Carry the lifetime
			// total here as "known"; Collect converts it to a per-window DELTA
			// against the prior reading (a single cumulative reading is not, by
			// itself, evidence of a NEW error this window).
			if s.value >= 0 {
				a.dw.ECCDoubleBitErrs = uint64(s.value)
				a.dw.ECCDoubleBitKnown = true
			}
		default:
			// Cumulative link-error counters (NVLink CRC/replay + PCIe replay) are
			// SUMMED per device into the lifetime total carried here as "known";
			// Collect converts the sum to a per-window DELTA against the prior
			// reading (mirror of the ECC-DBE pattern). A single reading is not, by
			// itself, evidence of a NEW link error this window.
			if dcgmLinkErrSymbols[s.symbol] && s.value >= 0 {
				a.dw.LinkErrors += uint64(s.value)
				a.dw.LinkErrorsKnown = true
			}
		}
	}

	obs := Observation{
		Source: gpufleetv1.SignalSource_SIGNAL_SOURCE_DCGM,
		Provenance: map[string]string{
			"source":            "dcgm-exporter-scrape",
			"exposition_fields": strconv.Itoa(len(samples)),
		},
	}

	// Deterministic ordering by UUID, then symbol.
	uuids := make([]string, 0, len(devs))
	for u := range devs {
		uuids = append(uuids, u)
	}
	sort.Strings(uuids)
	for _, u := range uuids {
		obs.DeviceWindows = append(obs.DeviceWindows, devs[u].dw)
	}
	skeys := make([]string, 0, len(series))
	for k := range series {
		skeys = append(skeys, k)
	}
	sort.Strings(skeys)
	for _, k := range skeys {
		obs.DcgmSeries = append(obs.DcgmSeries, series[k])
	}
	return obs, nil
}

// scanExposition parses Prometheus text-exposition lines into samples. It
// ignores HELP/TYPE comments and blank lines, and parses the
// `metric{label="v",...} value [timestamp]` grammar. Malformed lines are
// skipped (degrade), never fatal.
func scanExposition(r io.Reader) ([]dcgmParsedSample, error) {
	var out []dcgmParsedSample
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, value, ok := parseExpositionLine(line)
		if !ok {
			continue
		}
		out = append(out, dcgmParsedSample{symbol: name, labels: labels, value: value})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("agent: scan dcgm exposition: %w", err)
	}
	return out, nil
}

// parseExpositionLine parses one exposition line into (name, labels, value).
func parseExpositionLine(line string) (string, map[string]string, float64, bool) {
	name := line
	labels := map[string]string{}

	if i := strings.IndexByte(line, '{'); i >= 0 {
		name = line[:i]
		rest := line[i+1:]
		j := strings.IndexByte(rest, '}')
		if j < 0 {
			return "", nil, 0, false
		}
		labelStr := rest[:j]
		labels = parseLabels(labelStr)
		line = strings.TrimSpace(rest[j+1:])
	} else {
		// No labels: split name and value on whitespace.
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return "", nil, 0, false
		}
		name = fields[0]
		line = fields[1]
	}

	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "", nil, 0, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", nil, 0, false
	}
	return strings.TrimSpace(name), labels, v, true
}

// parseLabels parses a `k="v",k2="v2"` label string. It handles escaped quotes
// minimally (DCGM-exporter values do not contain commas in practice).
func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for _, kv := range splitLabels(s) {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		k := strings.TrimSpace(kv[:eq])
		v := strings.TrimSpace(kv[eq+1:])
		v = strings.TrimPrefix(v, `"`)
		v = strings.TrimSuffix(v, `"`)
		out[k] = v
	}
	return out
}

// splitLabels splits a label list on commas that are not inside quotes.
func splitLabels(s string) []string {
	var parts []string
	var b strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch ch {
		case '"':
			inQuote = !inQuote
			b.WriteByte(ch)
		case ',':
			if inQuote {
				b.WriteByte(ch)
			} else {
				parts = append(parts, b.String())
				b.Reset()
			}
		default:
			b.WriteByte(ch)
		}
	}
	if b.Len() > 0 {
		parts = append(parts, b.String())
	}
	return parts
}
