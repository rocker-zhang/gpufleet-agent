package agent

import (
	"encoding/json"
	"net/http"
	"time"

	semantics "github.com/rocker-zhang/gpufleet-semantics"
	"google.golang.org/protobuf/encoding/protojson"
)

// API is the agent's LOCAL, READ-ONLY HTTP surface (D-0010 Endpoint 1). It
// serves the latest normalized SignalSchema window and the per-device/per-job
// standalone cost wedge for the OPEN cli bypass viewer. It is READ-ONLY by
// construction: every handler rejects any method other than GET (zero write
// back, zero control, zero scheduling/throttling/kill — RULES §A). It NEVER
// originates egress to the controlplane; the cli reads it off-path.
type API struct {
	d *Daemon
}

// NewAPI wraps a Daemon as a read-only HTTP API.
func NewAPI(d *Daemon) *API { return &API{d: d} }

// Handler returns the read-only mux. Endpoints:
//
//	GET /healthz       — liveness; reports refresh count + error count.
//	GET /signals       — the latest gpufleet.v1 EvidencePack (SignalSchema window).
//	GET /cost          — the latest standalone cost wedge (per-device + per-job).
//	GET /window        — combined: window meta + degradation marks + cost summary.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.readOnly(a.handleHealth))
	mux.HandleFunc("/signals", a.readOnly(a.handleSignals))
	mux.HandleFunc("/cost", a.readOnly(a.handleCost))
	mux.HandleFunc("/window", a.readOnly(a.handleWindow))
	return mux
}

// readOnly enforces the read-only contract: only GET (and HEAD) are allowed;
// any mutating verb is refused with 405. This is the structural guarantee that
// the local API can never write back or control anything.
func (a *API) readOnly(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "agent local API is read-only (GET only)", http.StatusMethodNotAllowed)
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// HealthResponse is the /healthz payload.
type HealthResponse struct {
	OK        bool      `json:"ok"`
	Refreshes uint64    `json:"refreshes"`
	Errors    uint64    `json:"errors"`
	RefreshAt time.Time `json:"refresh_at,omitempty"`
}

func (a *API) handleHealth(w http.ResponseWriter, _ *http.Request) {
	st := a.d.Snapshot()
	resp := HealthResponse{OK: true, Errors: a.d.Errs()}
	if st != nil {
		resp.Refreshes = st.Refreshes
		resp.RefreshAt = st.RefreshAt
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleSignals serves the latest EvidencePack as canonical protojson so the
// cli (or any consumer) sees the real gpufleet.v1 wire shape.
func (a *API) handleSignals(w http.ResponseWriter, _ *http.Request) {
	st := a.d.Snapshot()
	if st == nil || st.Window == nil {
		http.Error(w, "no signal window collected yet", http.StatusServiceUnavailable)
		return
	}
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(st.Window.Pack)
	if err != nil {
		http.Error(w, "marshal evidence pack: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

// DeviceCost is one device's cost wedge in the API's JSON shape.
type DeviceCost struct {
	UUID           string  `json:"uuid"`
	Node           string  `json:"node"`
	MFU            float64 `json:"mfu"`
	TensorActive   float64 `json:"tensor_active"`
	IdleFraction   float64 `json:"idle_fraction"`
	CostUSD        float64 `json:"cost_usd"`
	WastedUSD      float64 `json:"wasted_usd"`
	Priced         bool    `json:"priced"`
	LowUtilization bool    `json:"low_utilization"`
}

// JobCost is one job's aggregated cost wedge in the API's JSON shape.
type JobCost struct {
	JobID     string  `json:"job_id"`
	WastedUSD float64 `json:"wasted_usd"`
	Priced    bool    `json:"priced"`
	Devices   int     `json:"devices"`
}

// CostResponse is the /cost payload.
type CostResponse struct {
	Devices []DeviceCost `json:"devices"`
	Jobs    []JobCost    `json:"jobs"`
}

func costResponse(st *State) CostResponse {
	resp := CostResponse{}
	for _, d := range st.Cost.Devices {
		resp.Devices = append(resp.Devices, DeviceCost{
			UUID:           d.Device.Device.UUID,
			Node:           d.Device.Device.Node,
			MFU:            d.Device.MFU,
			TensorActive:   d.Device.TensorActive,
			IdleFraction:   d.IdleFraction,
			CostUSD:        d.Device.CostUSD,
			WastedUSD:      d.WastedUSD,
			Priced:         d.Impact.Computed,
			LowUtilization: d.LowUtilization,
		})
	}
	for _, j := range st.Cost.Jobs {
		resp.Jobs = append(resp.Jobs, JobCost{
			JobID:     j.Job.ID,
			WastedUSD: j.Impact.UsdWindow,
			Priced:    j.Impact.Computed,
			Devices:   len(j.Wedges),
		})
	}
	return resp
}

func (a *API) handleCost(w http.ResponseWriter, _ *http.Request) {
	st := a.d.Snapshot()
	if st == nil {
		http.Error(w, "no signal window collected yet", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, costResponse(st))
}

// WindowResponse is the /window combined read-only view.
type WindowResponse struct {
	ContractVersion string        `json:"contract_version"`
	AgentID         string        `json:"agent_id"`
	WindowStart     time.Time     `json:"window_start"`
	WindowEnd       time.Time     `json:"window_end"`
	Sources         []string      `json:"sources"`
	Degraded        []DegradeMark `json:"degraded"`
	Cost            CostResponse  `json:"cost"`
}

func (a *API) handleWindow(w http.ResponseWriter, _ *http.Request) {
	st := a.d.Snapshot()
	if st == nil || st.Window == nil {
		http.Error(w, "no signal window collected yet", http.StatusServiceUnavailable)
		return
	}
	srcs := make([]string, 0, len(st.Window.Sources))
	for _, s := range st.Window.Sources {
		srcs = append(srcs, sourceShort(s))
	}
	resp := WindowResponse{
		ContractVersion: st.Window.Pack.ContractVersion,
		AgentID:         st.Window.Pack.AgentId,
		WindowStart:     st.Window.WindowStart,
		WindowEnd:       st.Window.WindowEnd,
		Sources:         srcs,
		Degraded:        st.Window.Degraded,
		Cost:            costResponse(st),
	}
	writeJSON(w, http.StatusOK, resp)
}

// DefaultCostPolicy re-exports the semantics default so callers/tests can build
// a Daemon without importing semantics directly.
func DefaultCostPolicy() semantics.CostPolicy { return semantics.DefaultCostPolicy() }
