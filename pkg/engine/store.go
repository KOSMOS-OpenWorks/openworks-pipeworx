package engine

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

// StoreJob represents a job in the backend store (Oracle, Postgres, etc.).
// This is the native store format — not the pipeworx Job struct.
type StoreJob struct {
	JID        int64  `json:"jid"`
	ServiceID  int    `json:"serviceId"`
	Parameter  string `json:"parameter"`
	State      int    `json:"state"`
	Priority   int    `json:"priority"`
	Result     string `json:"result,omitempty"`
	StartTime  string `json:"startTime,omitempty"`
	EventBegin string `json:"eventBegin,omitempty"`
	EventEnd   string `json:"eventEnd,omitempty"`
	EventError string `json:"eventError,omitempty"`
}

// StoreLogEntry represents a log entry from the backend store.
type StoreLogEntry struct {
	JID         int64  `json:"jid"`
	Description string `json:"description"`
	Code        int    `json:"code"`
	LogDate     string `json:"logDate"`
}

// StoreService represents a service type in the backend store.
type StoreService struct {
	ServiceID   int    `json:"serviceId"`
	Description string `json:"description"`
	BasePrio    int    `json:"basePrio"`
}

// StoreProcessor represents a processor heartbeat record.
type StoreProcessor struct {
	ServiceID int    `json:"serviceId"`
	ProcGUID  string `json:"procGuid"`
	ProcHost  string `json:"procHost"`
	LastCheck string `json:"lastCheck"`
}

// StoreInfo describes the backend store type and connection.
type StoreInfo struct {
	Type   string `json:"type"`              // "oracle", "postgres", "memory"
	DSN    string `json:"dsn,omitempty"`     // masked connection string
	Status string `json:"status"`            // "connected", "disconnected"
}

// StoreProvider is an optional interface for backend job store access.
// If set on the engine, it enables the /api/v0/store/* endpoints.
// OpenCloud does not implement this. XIS provides it via OracleStore/PostgresStore.
type StoreProvider interface {
	// ListJobs returns jobs from the backend store, optionally filtered by state.
	ListJobs(state *int, limit int) ([]StoreJob, error)

	// GetJob returns a single job with its log entries.
	GetJob(jid int64) (*StoreJob, []StoreLogEntry, error)

	// InjectJob creates a new job in the backend store (simulates webapp).
	InjectJob(serviceID int, parameter string, priority int) (int64, error)

	// ResetJob sets a job's state back to NEW (0) for re-processing.
	ResetJob(jid int64) error

	// ListServices returns all known service types.
	ListServices() ([]StoreService, error)

	// ListProcessors returns all processor heartbeat records.
	ListProcessors() ([]StoreProcessor, error)

	// Info returns backend store type and connection info.
	Info() StoreInfo
}

// SetStoreProvider enables the /api/v0/store/* endpoints.
func (e *JobEngine) SetStoreProvider(sp StoreProvider) {
	e.storeProvider = sp
}

// RegisterStoreRoutes adds the /api/v0/store/* routes if a StoreProvider is set.
// Called from RegisterRoutes — routes return 404 if no provider is configured.
func (e *JobEngine) registerStoreRoutes(r chi.Router) {
	r.Route("/api/v0/store", func(r chi.Router) {
		r.Get("/info", e.handleStoreInfo)
		r.Get("/jobs", e.handleStoreListJobs)
		r.Post("/jobs", e.handleStoreInjectJob)
		r.Get("/jobs/{jid}", e.handleStoreGetJob)
		r.Post("/jobs/{jid}/reset", e.handleStoreResetJob)
		r.Get("/services", e.handleStoreListServices)
		r.Get("/processors", e.handleStoreListProcessors)
	})
}

func (e *JobEngine) storeOrNotFound(w http.ResponseWriter) StoreProvider {
	if e.storeProvider == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no backend store configured"})
		return nil
	}
	return e.storeProvider
}

func (e *JobEngine) handleStoreInfo(w http.ResponseWriter, r *http.Request) {
	sp := e.storeOrNotFound(w)
	if sp == nil {
		return
	}
	writeJSON(w, http.StatusOK, sp.Info())
}

func (e *JobEngine) handleStoreListJobs(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	sp := e.storeOrNotFound(w)
	if sp == nil {
		return
	}

	var stateFilter *int
	if s := r.URL.Query().Get("state"); s != "" {
		v, err := strconv.Atoi(s)
		if err == nil {
			stateFilter = &v
		}
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 1000 {
			limit = v
		}
	}

	jobs, err := sp.ListJobs(stateFilter, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (e *JobEngine) handleStoreGetJob(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	sp := e.storeOrNotFound(w)
	if sp == nil {
		return
	}

	jid, err := strconv.ParseInt(chi.URLParam(r, "jid"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid jid"})
		return
	}

	job, logs, err := sp.GetJob(jid)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "logs": logs})
}

func (e *JobEngine) handleStoreInjectJob(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	sp := e.storeOrNotFound(w)
	if sp == nil {
		return
	}

	var req struct {
		ServiceID int    `json:"service_id"`
		Parameter string `json:"parameter"`
		Priority  int    `json:"priority"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}
	if req.ServiceID == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "service_id required"})
		return
	}

	jid, err := sp.InjectJob(req.ServiceID, req.Parameter, req.Priority)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"jid":     jid,
		"state":   0,
		"created": time.Now().Format(time.RFC3339),
	})
}

func (e *JobEngine) handleStoreResetJob(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	sp := e.storeOrNotFound(w)
	if sp == nil {
		return
	}

	jid, err := strconv.ParseInt(chi.URLParam(r, "jid"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid jid"})
		return
	}
	if err := sp.ResetJob(jid); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jid": jid, "state": 0})
}

func (e *JobEngine) handleStoreListServices(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	sp := e.storeOrNotFound(w)
	if sp == nil {
		return
	}

	services, err := sp.ListServices()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"services": services})
}

func (e *JobEngine) handleStoreListProcessors(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	sp := e.storeOrNotFound(w)
	if sp == nil {
		return
	}

	processors, err := sp.ListProcessors()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"processors": processors})
}

// isAdmin checks if the request is from an admin user.
func (e *JobEngine) isAdmin(r *http.Request) bool {
	user, ok := e.auth.ExtractUser(r)
	return ok && user.IsAdmin
}
