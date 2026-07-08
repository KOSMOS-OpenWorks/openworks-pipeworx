package engine

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"codeberg.org/kosmos-openworks/openworks-pipeworx/pkg/config"
)

var validIDRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// RegisterRoutes sets up the HTTP API routes
func (e *JobEngine) RegisterRoutes(r chi.Router) {
	r.Route("/api/v0/jobs", func(r chi.Router) {
		// User-facing API
		r.Get("/pipelines", e.handleGetPipelines)
		r.Post("/", e.handleSubmitJob)
		r.Get("/all", e.handleListAllJobs)
		r.Get("/{jobId}", e.handleGetJob)
		r.Delete("/{jobId}", e.handleCancelJob)
		r.Get("/", e.handleListJobs)

		// Worker-facing API (OpenWorks protocol)
		r.Route("/workers", func(r chi.Router) {
			r.Post("/poll", e.handleWorkerPoll)
			r.Get("/", e.handleListWorkers)
		})

		// Pipeline stats
		r.Get("/stats", e.handleJobStats)
	})

	// Admin API for Pipe-Matrix
	e.RegisterMatrixRoutes(r)

	// Optional backend store API (/api/v0/store/*)
	// Returns 404 if no StoreProvider is configured.
	e.registerStoreRoutes(r)
}

// PipelineInfo is the public representation of a pipeline
type PipelineInfo struct {
	ID             string               `json:"id"`
	Label          string               `json:"label"`
	Icon           string               `json:"icon"`
	SourceTypes    []string             `json:"sourceTypes"`
	TargetLocation string               `json:"targetLocation"`
	Menu           string               `json:"menu,omitempty"`
	Dialog         *config.DialogSpec   `json:"dialog,omitempty"`
	Shares         *config.SharesConfig `json:"shares,omitempty"`
	JobType        string               `json:"jobType"`
	Notification   string               `json:"notification,omitempty"`
	DesignedBy     string               `json:"designedBy,omitempty"`
}

type PipelinesResponse struct {
	Pipelines []PipelineInfo `json:"pipelines"`
}

type SubmitRequest struct {
	Pipeline     string   `json:"pipeline"`
	Resources    []string `json:"resources"`
	TargetPath   string   `json:"targetPath"`
	CreateTarget bool     `json:"createTarget"`
	Params       any      `json:"params,omitempty"`
	Priority     int      `json:"priority,omitempty"`
	ETA          string   `json:"eta,omitempty"`
	DependsOn    []string `json:"dependsOn,omitempty"`
}

func (e *JobEngine) handleGetPipelines(w http.ResponseWriter, r *http.Request) {
	pipelines := e.Pipelines()
	resp := PipelinesResponse{
		Pipelines: make([]PipelineInfo, 0, len(pipelines)),
	}

	for id, p := range pipelines {
		resp.Pipelines = append(resp.Pipelines, PipelineInfo{
			ID:             id,
			Label:          p.Label,
			Icon:           p.Icon,
			SourceTypes:    p.SourceTypes,
			TargetLocation: p.Target.Location,
			Menu:           p.Menu,
			Dialog:         p.Dialog,
			Shares:         p.Shares,
			JobType:        p.Job.Type,
			Notification:   p.Notification,
			DesignedBy:     p.DesignedBy,
		})
	}

	writeJSON(w, http.StatusOK, resp)
}

func (e *JobEngine) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	// Validate pipeline ID — only alphanumeric, dash, underscore
	if req.Pipeline == "" || !validIDRe.MatchString(req.Pipeline) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid pipeline id"})
		return
	}

	if len(req.Resources) > 1000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max 1000 resources"})
		return
	}

	// Validate target path — prevent path traversal
	if req.TargetPath != "" {
		cleaned := filepath.Clean(req.TargetPath)
		if strings.Contains(cleaned, "..") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid target path"})
			return
		}
		req.TargetPath = cleaned
	}

	userInfo, ok := e.auth.ExtractUser(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	userID := userInfo.ID

	// Rate limit: max 10 active jobs per user
	activeJobs := e.GetUserJobs(userID, "")
	activeCount := 0
	for _, j := range activeJobs {
		if j.Status == StatusQueued || j.Status == StatusRunning {
			activeCount++
		}
	}
	if activeCount >= 10 {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "max 10 active jobs per user"})
		return
	}

	var eta time.Time
	if req.ETA != "" {
		eta, _ = time.Parse(time.RFC3339, req.ETA)
	}

	job, err := e.Submit(req.Pipeline, req.Resources, userID, req.TargetPath, req.CreateTarget, &SubmitOpts{
		Params:    req.Params,
		Priority:  req.Priority,
		ETA:       eta,
		DependsOn: req.DependsOn,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusAccepted, job)
}

func (e *JobEngine) handleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	job, ok := e.GetJob(jobID)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (e *JobEngine) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	if err := e.CancelJob(jobID); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (e *JobEngine) handleListJobs(w http.ResponseWriter, r *http.Request) {
	userID := ""
	if userInfo, ok := e.auth.ExtractUser(r); ok {
		userID = userInfo.ID
	}
	status := JobStatus(r.URL.Query().Get("status"))

	jobs := e.GetUserJobs(userID, status)
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (e *JobEngine) handleListAllJobs(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		http.Error(w, "admin required", http.StatusForbidden)
		return
	}
	status := JobStatus(r.URL.Query().Get("status"))
	jobs := e.GetUserJobs("", status)
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (e *JobEngine) handleJobStats(w http.ResponseWriter, r *http.Request) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := make(map[string]map[string]int) // pipeline → { status → count }
	for _, job := range e.jobs {
		if stats[job.Pipeline] == nil {
			stats[job.Pipeline] = make(map[string]int)
		}
		stats[job.Pipeline][string(job.Status)]++
	}

	writeJSON(w, http.StatusOK, map[string]any{"stats": stats})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
