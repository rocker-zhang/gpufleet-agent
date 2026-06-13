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
//
// LastSuccessAt / AgeSeconds / Stale / StaleReason reflect the agent-side data
// freshness (TASK-0040): last_success_at is the last SUCCESSFUL collection (==
// refresh_at of the live window), age_seconds is how long ago that was, and
// stale=true means the agent is still serving that last-known window but it is
// older than the staleness threshold (so a consumer must NOT treat it as live).
// OK stays true even when stale — the agent process is healthy and off-path; a
// stale data window is a data-freshness signal, not a liveness failure.
//
// NeverCollected (TASK-0041) marks the MOST-stale case: the agent has not once
// successfully scraped metrics (e.g. exporter unreachable from startup). It is
// reported stale=true with NO last_success_at (there is none — the field is
// omitted rather than a misleading zero time) and age_seconds measured SINCE
// STARTUP, never a misleading 0. A never-collected agent is never reported fresh.
type HealthResponse struct {
	OK        bool   `json:"ok"`
	Refreshes uint64 `json:"refreshes"`
	Errors    uint64 `json:"errors"`
	// RefreshAt / LastSuccessAt are POINTERS so a never-collected agent OMITS them
	// on the wire entirely (a value-typed time.Time always serializes its zero
	// "0001-01-01T00:00:00Z", which would read as a misleading collection time —
	// TASK-0041). nil ⇒ absent.
	RefreshAt      *time.Time `json:"refresh_at,omitempty"`
	LastSuccessAt  *time.Time `json:"last_success_at,omitempty"`
	AgeSeconds     float64    `json:"age_seconds"`
	Stale          bool       `json:"stale"`
	NeverCollected bool       `json:"never_collected"`
	StaleReason    string     `json:"stale_reason,omitempty"`
	ConsecFailures uint64     `json:"consec_failures"`
}

func (a *API) handleHealth(w http.ResponseWriter, _ *http.Request) {
	st := a.d.Snapshot()
	fr := a.d.Freshness()
	resp := HealthResponse{
		OK:             true,
		Errors:         a.d.Errs(),
		Stale:          fr.Stale,
		NeverCollected: fr.NeverCollected,
		StaleReason:    fr.Reason,
		ConsecFailures: fr.ConsecFails,
	}
	if st != nil {
		resp.Refreshes = st.Refreshes
		ra := st.RefreshAt
		resp.RefreshAt = &ra
	}
	if fr.HasData {
		// Last-known cost-bearing collection: report its time + age.
		ca := fr.CollectedAt
		resp.LastSuccessAt = &ca
		resp.AgeSeconds = fr.Age.Seconds()
	} else if fr.NeverCollected {
		// Never collected (TASK-0041): there is NO last_success_at — omit it rather
		// than emit a misleading zero time. Report age SINCE STARTUP so age_seconds
		// is never a misleading 0 that reads as "just collected".
		resp.AgeSeconds = fr.Age.Seconds()
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
//
// wasted_usd is the per-WINDOW attributed waste (CostUSD * idle over this
// window); usd_per_hour is the per-HOUR burn RATE if the same idle condition
// persists (semantics CostImpact.UsdPerHour = CostPerHour * idle). The two are
// distinct quantities: for a sub-hour window an idle device's usd_per_hour
// exceeds its wasted_usd (a rate vs. a window slice). Both are meaningful ONLY
// when priced==true (semantics CostImpact.Computed); when priced==false they
// are zero and carry NO meaning — the cli renders a degrade mark, not $0.
type DeviceCost struct {
	UUID           string  `json:"uuid"`
	Node           string  `json:"node"`
	MFU            float64 `json:"mfu"`
	TensorActive   float64 `json:"tensor_active"`
	IdleFraction   float64 `json:"idle_fraction"`
	CostUSD        float64 `json:"cost_usd"`
	WastedUSD      float64 `json:"wasted_usd"`
	UsdPerHour     float64 `json:"usd_per_hour"`
	Priced         bool    `json:"priced"`
	LowUtilization bool    `json:"low_utilization"`
}

// JobCost is one job's aggregated cost wedge in the API's JSON shape.
//
// wasted_usd / usd_per_hour mirror the per-device meaning, summed across the
// job's priced devices (semantics aggregate CostImpact.UsdWindow / UsdPerHour).
type JobCost struct {
	JobID      string  `json:"job_id"`
	WastedUSD  float64 `json:"wasted_usd"`
	UsdPerHour float64 `json:"usd_per_hour"`
	Priced     bool    `json:"priced"`
	Devices    int     `json:"devices"`
}

// CostResponse is the /cost payload.
//
// CollectedAt / AgeSeconds / Stale / StaleReason are the data-freshness fields
// (TASK-0040), reported at the TOP level (not per-device) because freshness is a
// property of the whole window, not of one device. collected_at is the
// last-SUCCESSFUL collection time, age_seconds is how old that is at read time,
// and stale=true means the device/job values BELOW are the last-known window
// held past the staleness threshold — they are kept (never blanked, never
// fabricated) but MUST be presented as stale, not current (RULES §B). When fresh,
// stale=false and stale_reason is empty; the device/job fields are unchanged from
// before this task (backward compatible — the per-device DTOs did not change).
// NeverCollected (TASK-0041) is the MOST-stale case: the agent has not once
// successfully scraped metrics, so there is no last-known window to serve at all.
// On never-collected /cost returns 200 (NOT 503) with Devices/Jobs empty,
// Stale=true, NeverCollected=true, and a populated StaleReason — self-consistent,
// machine-readable, and rendered by the cli as a clear "not collected yet"
// message rather than a raw HTTP error or a silent blank (RULES §B).
type CostResponse struct {
	Devices []DeviceCost `json:"devices"`
	Jobs    []JobCost    `json:"jobs"`
	// CollectedAt is a POINTER so a never-collected response OMITS it (a value-typed
	// time.Time always serializes its zero "0001-01-01T00:00:00Z", a misleading
	// fake collection time — TASK-0041). nil ⇒ absent; the cli's value-typed mirror
	// simply stays zero when the field is absent (wire-compatible).
	CollectedAt    *time.Time `json:"collected_at,omitempty"`
	AgeSeconds     float64    `json:"age_seconds"`
	Stale          bool       `json:"stale"`
	NeverCollected bool       `json:"never_collected"`
	StaleReason    string     `json:"stale_reason,omitempty"`
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
			UsdPerHour:     d.Impact.UsdPerHour,
			Priced:         d.Impact.Computed,
			LowUtilization: d.LowUtilization,
		})
	}
	for _, j := range st.Cost.Jobs {
		resp.Jobs = append(resp.Jobs, JobCost{
			JobID:      j.Job.ID,
			WastedUSD:  j.Impact.UsdWindow,
			UsdPerHour: j.Impact.UsdPerHour,
			Priced:     j.Impact.Computed,
			Devices:    len(j.Wedges),
		})
	}
	return resp
}

// stampFreshness copies the daemon's current freshness verdict onto a
// CostResponse (TASK-0040). Centralized so /cost and /window report the same
// data-age + stale fields and never present a held-stale window as current.
func stampFreshness(resp CostResponse, fr Freshness) CostResponse {
	resp.Stale = fr.Stale
	resp.NeverCollected = fr.NeverCollected
	resp.StaleReason = fr.Reason
	if fr.HasData {
		// Age of the last-known window + its (real) collection time.
		ca := fr.CollectedAt
		resp.CollectedAt = &ca
		resp.AgeSeconds = fr.Age.Seconds()
	} else if fr.NeverCollected {
		// Age SINCE STARTUP (never a misleading 0). CollectedAt is OMITTED — there is
		// no successful collection time to report (never a zero-time fake).
		resp.AgeSeconds = fr.Age.Seconds()
	}
	return resp
}

func (a *API) handleCost(w http.ResponseWriter, _ *http.Request) {
	// Serve the last COST-bearing window (TASK-0040): if the exporter is currently
	// unreachable, this is the last-known device data — retained, never blanked —
	// and stampFreshness flags it stale so the reader never treats it as live.
	st := a.d.CostSnapshot()
	if st == nil {
		// NEVER COLLECTED (TASK-0041): no successful metrics scrape ever, so there is
		// no last-known window. Return 200 (NOT 503) with empty devices/jobs +
		// stale=true + never_collected=true + a populated reason — a self-consistent,
		// machine-readable empty state the cli renders as a clear "agent has not
		// collected data yet" message (never a raw HTTP error, never a silent blank).
		writeJSON(w, http.StatusOK, stampFreshness(CostResponse{}, a.d.Freshness()))
		return
	}
	writeJSON(w, http.StatusOK, stampFreshness(costResponse(st), a.d.Freshness()))
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
	// Window meta comes from the freshest window; the Cost block comes from the
	// last COST-bearing window (TASK-0040) + the freshness verdict, so a
	// metrics-less cycle never blanks the cost view nor presents it as live.
	costSt := a.d.CostSnapshot()
	if costSt == nil {
		costSt = st
	}
	resp := WindowResponse{
		ContractVersion: st.Window.Pack.ContractVersion,
		AgentID:         st.Window.Pack.AgentId,
		WindowStart:     st.Window.WindowStart,
		WindowEnd:       st.Window.WindowEnd,
		Sources:         srcs,
		Degraded:        st.Window.Degraded,
		Cost:            stampFreshness(costResponse(costSt), a.d.Freshness()),
	}
	writeJSON(w, http.StatusOK, resp)
}

// DefaultCostPolicy re-exports the semantics default so callers/tests can build
// a Daemon without importing semantics directly.
func DefaultCostPolicy() semantics.CostPolicy { return semantics.DefaultCostPolicy() }
