package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	gpufleetv1 "github.com/rocker-zhang/gpufleet-proto/gen/go/gpufleet/v1"
)

// This file is the DEFAULT (no-GPU) build's METRICS-PRIMARY collector: a
// PromQL HTTP-API reader that queries the customer's EXISTING Prometheus and
// reuses already-scraped fields. It adds ZERO new exporter/scrape load — it is a
// pure HTTP GET against the instant-query endpoint, off the GPU critical path
// (RULES §A). It NEVER writes back, controls, or schedules anything. A query
// error is returned to the daemon, which counts it and degrades (never crashes
// the node or affects a customer job).
//
// It is testable WITHOUT real hardware or a real Prometheus: point BaseURL at an
// httptest server that serves the Prometheus instant-query JSON shape.

// PromQueries names the PromQL expressions the collector reads from the
// customer's existing Prometheus. Each is an INSTANT query over already-scraped
// series; the collector never defines recording rules or pushes load. Empty
// expressions are skipped (the field is then absent → normalizer degrades).
type PromQueries struct {
	// TensorActive yields DCGM_FI_PROF_PIPE_TENSOR_ACTIVE (ratio) keyed by a
	// device-UUID label. Used for tensor-active seconds over the window.
	TensorActive string
	// AchievedFLOPs yields achieved FLOP/s (or a ratio*peak) keyed by UUID.
	AchievedFLOPs string
	// PeakFLOPS yields the device advertised peak FLOP/s keyed by UUID.
	PeakFLOPS string
	// CostPerHour yields the $/hour billing rate keyed by UUID.
	CostPerHour string
	// JobOwner yields the device→job ownership (value carried in the `job` label)
	// keyed by UUID. Used to resolve the device→job mapping.
	JobOwner string
	// ECCDoubleBit yields the per-WINDOW INCREASE of the DCGM uncorrectable
	// (double-bit) ECC counter (DCGM_FI_DEV_ECC_DBE_VOL_TOTAL) keyed by UUID. It is
	// the Prometheus-primary leg of the ECC-uncorrectable gate: an increase>0 over
	// the window, corroborated by an INDEPENDENT kernel/dmesg ECC Xid, fires
	// FAULT_CLASS_ECC_UNCORRECTABLE on a Prometheus-primary node. The expression
	// MUST return the per-window DELTA (e.g. increase(<counter>[<range>])), NOT the
	// lifetime total — a single cumulative reading is not, by itself, evidence of a
	// NEW error this window (honesty, RULES §B). The collector treats the value as
	// the already-computed delta and emits the leg only when it is > 0.
	ECCDoubleBit string
}

// PromLabels names the Prometheus label keys the collector reads identity from.
// Defaults match the common DCGM-exporter labeling (UUID, Hostname, modelName).
type PromLabels struct {
	UUID  string // default "UUID"
	Node  string // default "Hostname"
	Model string // default "modelName"
	Job   string // default "exported_job" then "job"
}

func (l PromLabels) withDefaults() PromLabels {
	if l.UUID == "" {
		l.UUID = "UUID"
	}
	if l.Node == "" {
		l.Node = "Hostname"
	}
	if l.Model == "" {
		l.Model = "modelName"
	}
	if l.Job == "" {
		l.Job = "job"
	}
	return l
}

// PrometheusCollector reads metrics from an EXISTING Prometheus via the PromQL
// HTTP instant-query API. It is the DEFAULT metrics source (zero new scrape
// load). It implements the read-only Collector interface.
type PrometheusCollector struct {
	// BaseURL is the Prometheus server root (e.g. "http://prometheus:9090"). In
	// tests this is an httptest.Server URL. Required.
	BaseURL string
	// Node, when set, scopes/labels observations to this node (per-node DaemonSet
	// mode). Empty means the central agentless query mode (all nodes the query
	// returns), and identity is taken from the series labels.
	Node string
	// Queries are the PromQL expressions to read. Empty fields are skipped.
	Queries PromQueries
	// Labels names the identity label keys. Zero value uses sensible defaults.
	Labels PromLabels
	// HTTPClient is the client used for queries. Nil uses a short-timeout default.
	HTTPClient *http.Client
	// Timeout bounds each query. Zero uses 5s. Read-only + bounded so a slow
	// Prometheus can never stall the daemon.
	Timeout time.Duration
}

func (c PrometheusCollector) Source() gpufleetv1.SignalSource {
	return gpufleetv1.SignalSource_SIGNAL_SOURCE_PROMETHEUS
}

func (c PrometheusCollector) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	to := c.Timeout
	if to <= 0 {
		to = 5 * time.Second
	}
	return &http.Client{Timeout: to}
}

// promInstantResponse is the Prometheus instant-query JSON envelope. Only the
// vector result type is consumed (instant queries return a vector).
type promInstantResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string             `json:"resultType"`
		Result     []promVectorSample `json:"result"`
	} `json:"data"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

type promVectorSample struct {
	Metric map[string]string `json:"metric"`
	// Value is [ <unix_ts float>, "<string value>" ].
	Value []json.RawMessage `json:"value"`
}

func (s promVectorSample) scalar() (float64, bool) {
	if len(s.Value) != 2 {
		return 0, false
	}
	var str string
	if err := json.Unmarshal(s.Value[1], &str); err != nil {
		return 0, false
	}
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// query runs one instant PromQL query and returns the vector samples. A
// transport error, non-200, or Prometheus-reported error becomes a Go error so
// the daemon counts+degrades. It is a pure GET — zero write-back.
func (c PrometheusCollector) query(ctx context.Context, expr string, ts time.Time) ([]promVectorSample, error) {
	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("agent: bad prometheus BaseURL: %w", err)
	}
	u.Path = u.Path + "/api/v1/query"
	q := u.Query()
	q.Set("query", expr)
	q.Set("time", strconv.FormatFloat(float64(ts.UnixNano())/1e9, 'f', 3, 64))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("agent: prometheus query: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent: prometheus query status %d: %s", resp.StatusCode, string(body))
	}
	var pr promInstantResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("agent: decode prometheus response: %w", err)
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("agent: prometheus error %s: %s", pr.ErrorType, pr.Error)
	}
	return pr.Data.Result, nil
}

// promDevAccum accumulates per-device fields read across the several queries.
type promDevAccum struct {
	dw      DeviceWindow
	job     string
	jobSeen bool
}

// Collect reads the configured PromQL queries from the existing Prometheus and
// returns a source-tagged Observation. Fields the queries did not return are
// left unset (the normalizer marks them missing — degrade, never fabricate).
func (c PrometheusCollector) Collect(now time.Time, window time.Duration) (Observation, error) {
	if c.BaseURL == "" {
		return Observation{}, fmt.Errorf("agent: PrometheusCollector requires BaseURL")
	}
	to := c.Timeout
	if to <= 0 {
		to = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), to)
	defer cancel()

	labels := c.Labels.withDefaults()
	ws := window.Seconds()
	devs := map[string]*promDevAccum{}

	get := func(uuid, node, model string) *promDevAccum {
		a, ok := devs[uuid]
		if !ok {
			a = &promDevAccum{dw: DeviceWindow{UUID: uuid, WindowSeconds: ws}}
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

	ident := func(m map[string]string) (uuid, node, model string) {
		uuid = m[labels.UUID]
		node = m[labels.Node]
		model = m[labels.Model]
		if node == "" {
			node = c.Node
		}
		return
	}

	// Each query is independent: a failing/empty one degrades only its field.
	// We surface the FIRST transport error so the daemon counts it, but still
	// emit whatever other queries returned (partial → normalizer degrades rest).
	var firstErr error
	run := func(expr string, apply func(a *promDevAccum, v float64)) {
		if expr == "" {
			return
		}
		samples, err := c.query(ctx, expr, now)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return
		}
		for _, s := range samples {
			uuid, node, model := ident(s.Metric)
			if uuid == "" {
				continue // cannot attribute a device-less sample
			}
			v, ok := s.scalar()
			if !ok {
				continue
			}
			apply(get(uuid, node, model), v)
		}
	}

	run(c.Queries.PeakFLOPS, func(a *promDevAccum, v float64) {
		a.dw.PeakFLOPS, a.dw.PeakFLOPSKnown = v, true
	})
	run(c.Queries.CostPerHour, func(a *promDevAccum, v float64) {
		a.dw.CostPerHour, a.dw.CostKnown = v, true
	})
	run(c.Queries.AchievedFLOPs, func(a *promDevAccum, v float64) {
		// Value is FLOP/s; convert to total FLOPs over the window.
		a.dw.AchievedFLOPs, a.dw.AchievedFLOPsKnown = v*ws, true
	})
	run(c.Queries.TensorActive, func(a *promDevAccum, v float64) {
		// Value is a ratio in [0,1]; convert to active-seconds over the window.
		a.dw.TensorActiveSecs, a.dw.TensorActiveKnown = v*ws, true
	})
	run(c.Queries.ECCDoubleBit, func(a *promDevAccum, v float64) {
		// Value is the per-WINDOW INCREASE of the uncorrectable (double-bit) ECC
		// counter (the query is responsible for the delta, e.g. increase(...[range])).
		// HONESTY (RULES §B): emit the leg only when the increase is genuinely > 0 —
		// a zero/absent increase is no new error this window (degrade, never
		// fabricate). Round FIRST, then mark known only on a rounded count >= 1: a
		// fractional delta in (0, 0.5) rounds to 0, and a "known" leg carrying a 0
		// count would contradict the emit-only-on-increase>0 contract (the
		// normalizer would mint a leg from a 0).
		if rounded := uint64(v + 0.5); rounded >= 1 {
			a.dw.ECCDoubleBitErrs = rounded
			a.dw.ECCDoubleBitKnown = true
		}
	})

	// JobOwner is read separately because the device→job value lives in a label.
	if c.Queries.JobOwner != "" {
		samples, err := c.query(ctx, c.Queries.JobOwner, now)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else {
			for _, s := range samples {
				uuid, node, model := ident(s.Metric)
				if uuid == "" {
					continue
				}
				a := get(uuid, node, model)
				if job := s.Metric[labels.Job]; job != "" {
					a.job, a.jobSeen = job, true
				}
			}
		}
	}

	obs := Observation{
		Source: c.Source(),
		Provenance: map[string]string{
			"endpoint":      c.BaseURL,
			"mode":          promMode(c.Node),
			"scrape_reused": "true", // zero new exporter load: instant query only
		},
	}
	// Deterministic ordering: sort device UUIDs.
	uuids := sortedKeys(devs)
	for _, u := range uuids {
		a := devs[u]
		obs.DeviceWindows = append(obs.DeviceWindows, a.dw)
		if a.jobSeen {
			obs.Mappings = append(obs.Mappings, &gpufleetv1.DeviceJobMapping{
				DeviceUuid:         u,
				Node:               a.dw.Node,
				JobId:              a.job,
				PeakTflops:         peakTflopsOrZero(a.dw),
				CostRateUsdPerHour: costOrZero(a.dw),
			})
		}
	}
	// A transport error with NO data at all is a hard collector error (daemon
	// counts it, falls back to DCGM). Partial data is returned for degradation.
	if firstErr != nil && len(obs.DeviceWindows) == 0 {
		return Observation{}, firstErr
	}
	return obs, nil
}

// sortedKeys returns the map keys sorted, for deterministic iteration.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func promMode(node string) string {
	if node == "" {
		return "central-agentless"
	}
	return "per-node-daemonset"
}

func peakTflopsOrZero(dw DeviceWindow) float64 {
	if dw.PeakFLOPSKnown {
		return dw.PeakFLOPS / 1e12
	}
	return 0
}

func costOrZero(dw DeviceWindow) float64 {
	if dw.CostKnown {
		return dw.CostPerHour
	}
	return 0
}
