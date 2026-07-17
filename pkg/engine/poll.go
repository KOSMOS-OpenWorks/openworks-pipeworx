package engine

import (
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/google/uuid"
	"codeberg.org/kosmos-openworks/openworks-pipeworx/pkg/config"
)

// PollRequest is the worker → cloud message in each poll tick
type PollRequest struct {
	Pick     []string           `json:"pick"`
	Capacity int                `json:"capacity"`
	Status   []WorkerJobStatus  `json:"status,omitempty"`
	Data     map[string]any     `json:"data,omitempty"`
	RegToken string             `json:"regToken,omitempty"`
}

// WorkerJobStatus is a progress/completion report from the worker
type WorkerJobStatus struct {
	JobID     string    `json:"jobId"`
	Progress  int       `json:"progress,omitempty"`
	Stage     string    `json:"stage,omitempty"`
	StageData any       `json:"stageData,omitempty"`
	Status    JobStatus `json:"status,omitempty"`
	Result    any       `json:"result,omitempty"`
	Error     string    `json:"error,omitempty"`
}

// PollResponse is the cloud → worker message in each poll tick
type PollResponse struct {
	Assign   []JobAssignment    `json:"assign"`
	Cancel   []string           `json:"cancel"`
	Slots    map[string]int     `json:"slots,omitempty"`
	Denied   []string           `json:"denied,omitempty"`
	RegToken *string            `json:"regToken"`
	Config   PollConfig         `json:"config"`
}

// JobAssignment is a single job assigned to a worker
type JobAssignment struct {
	JobID       string         `json:"jobId"`
	Job         JobDescription `json:"job"`
	Timeout     int            `json:"timeout"`
	ValidTill   string         `json:"validTill"`
	Origin      *ShareInfo     `json:"origin,omitempty"`
	Destination *ShareInfo     `json:"destination,omitempty"`
}

// JobDescription is the opaque job payload passed to the worker
type JobDescription struct {
	Type   string         `json:"type"`
	Params map[string]any `json:"params,omitempty"`
}

// ShareInfo represents an ephemeral share for a job
type ShareInfo struct {
	Type     string `json:"type"`               // "file" | "folder"
	WebDAVURL string `json:"webdav_url"`
	Token    string `json:"token"`
	Writable bool   `json:"writable"`
}

// PollConfig holds polling parameters sent to the worker
type PollConfig struct {
	PollIntervalMin int `json:"poll_interval_min"`
	PollIntervalMax int `json:"poll_interval_max"`
}

func (e *JobEngine) handleWorkerPoll(w http.ResponseWriter, r *http.Request) {
	userInfo, ok := e.auth.ExtractUser(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		return
	}
	workerID := userInfo.ID

	var req PollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	if req.Capacity <= 0 {
		req.Capacity = 1
	}

	// Rate limit: check poll frequency
	if e.isPollingTooFast(workerID) {
		w.WriteHeader(http.StatusTooManyRequests)
		return // no body — backpressure signal
	}

	// Record heartbeat + offered types + capacity
	e.recordHeartbeat(workerID)
	e.recordPick(workerID, req.Pick)
	e.recordCapacity(workerID, req.Capacity)

	// Process status reports from the worker
	for _, s := range req.Status {
		e.processWorkerStatus(workerID, s)
	}

	// regToken validation:
	// - Worker sends regToken → validate it
	// - Worker sends pipeline data (no token or invalid token) → register + issue new token
	// - regToken is in-memory, lost on engine restart → worker re-registers
	var regToken *string
	registeredNow := false

	if req.RegToken != "" {
		// Validate existing token
		e.mu.RLock()
		stored, ok := e.regTokens[workerID]
		e.mu.RUnlock()
		if ok && stored == req.RegToken {
			regToken = &req.RegToken
		}
		// If not ok → regToken stays nil → worker will re-register
	}

	// Process worker data (pipelines, logs, etc.)
	if req.Data != nil {
		e.processWorkerData(workerID, req.Data)

		// If pipelines were sent, issue a new regToken
		if _, hasPipelines := req.Data["pipelines"]; hasPipelines {
			newToken := uuid.New().String()
			e.mu.Lock()
			e.regTokens[workerID] = newToken
			// Re-registration: reset stale running jobs from this worker.
			// The worker restarted and no longer knows about these jobs.
			for _, job := range e.jobs {
				if job.WorkerID == workerID && job.Status == StatusRunning {
					job.Status = StatusQueued
					job.WorkerID = ""
					job.PickedAt = time.Time{}
				}
			}
			e.mu.Unlock()
			regToken = &newToken
			registeredNow = true
		}
	}

	// Determine allowed types from pipe matrix
	slots, denied := e.getWorkerSlots(workerID, req.Pick)

	// If nothing is allowed, return 403
	if len(slots) == 0 {
		writeJSON(w, http.StatusForbidden, map[string]any{
			"denied":   denied,
			"regToken": regToken,
		})
		return
	}

	// Find cancellations for this worker
	cancellations := e.getWorkerCancellations(workerID)

	// Pick jobs from queue
	assignments := e.pickJobs(workerID, slots, req.Capacity)

	resp := PollResponse{
		Assign:   assignments,
		Cancel:   cancellations,
		RegToken: regToken,
		Config: PollConfig{
			PollIntervalMin: e.cfg.Service.PollIntervalMin,
			PollIntervalMax: e.cfg.Service.PollIntervalMax,
		},
	}

	// Only include slots/denied on registration, not every poll
	if registeredNow {
		resp.Slots = slots
		resp.Denied = denied
	}

	writeJSON(w, http.StatusOK, resp)
}

// WorkerInfo represents a known worker for the admin API
type WorkerInfo struct {
	ID       string   `json:"id"`
	LastSeen string   `json:"lastSeen"`
	OnlineH  float64  `json:"onlineHours"`
	Online   bool     `json:"online"`
	Pick     []string `json:"pick,omitempty"`
	Capacity int      `json:"capacity"`          // max (last reported)
	Running  int      `json:"running"`           // currently assigned
}

// handleListWorkers returns all known workers with heartbeat info
func (e *JobEngine) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin required"})
		return
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Collect all known worker IDs from heartbeats + matrix
	known := make(map[string]bool)
	for id := range e.heartbeats {
		known[id] = true
	}
	for id := range e.pipeMatrix {
		known[id] = true
	}

	now := time.Now()
	maxInterval := time.Duration(e.cfg.Service.PollIntervalMax) * time.Second
	if maxInterval == 0 {
		maxInterval = 30 * time.Second
	}

	// Count running jobs per worker
	runningPerWorker := make(map[string]int)
	for _, job := range e.jobs {
		if job.Status == StatusRunning || job.Status == StatusQueued {
			runningPerWorker[job.WorkerID]++
		}
	}

	workers := make([]WorkerInfo, 0, len(known))
	for id := range known {
		info := WorkerInfo{ID: id}
		if last, ok := e.heartbeats[id]; ok {
			info.LastSeen = last.Format(time.RFC3339)
			info.Online = now.Sub(last) < maxInterval*2
			info.OnlineH = math.Round(now.Sub(last).Hours()*10) / 10
		}
		if pick, ok := e.workerPick[id]; ok {
			info.Pick = pick
		}
		info.Capacity = e.workerCap[id]
		info.Running = runningPerWorker[id]
		workers = append(workers, info)
	}

	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

// processWorkerStatus applies a worker's status report to the job
func (e *JobEngine) processWorkerStatus(workerID string, s WorkerJobStatus) {
	e.mu.Lock()
	defer e.mu.Unlock()

	job, ok := e.jobs[s.JobID]
	if !ok {
		return
	}

	// Only accept status from the worker that owns this job
	if job.WorkerID != workerID {
		return
	}

	if s.Progress > 0 {
		job.Progress = s.Progress
	}
	if s.Stage != "" {
		job.Stage = s.Stage
		job.StageData = s.StageData
	}

	if s.Status == StatusCompleted {
		job.Status = StatusCompleted
		job.Progress = 100
		job.Result = s.Result
		job.CompletedAt = time.Now()
	}

	if s.Status == StatusFailed {
		job.Error = s.Error
		job.Result = s.Result
		job.Retries++

		// Check max retries from pipeline config (default: 3)
		maxRetries := 3
		if p, ok := e.cfg.Pipelines[job.Pipeline]; ok && p.Job.MaxRetries > 0 {
			maxRetries = p.Job.MaxRetries
		}

		// Re-queue only if not expired AND retries not exhausted
		if job.ValidTill.After(time.Now()) && job.Retries < maxRetries {
			job.Status = StatusQueued
			job.WorkerID = ""
			job.PickedAt = time.Time{}
		} else {
			job.Status = StatusFailed
			job.CompletedAt = time.Now()
		}
	}
}

// processWorkerData handles opaque data from the worker (pipelines, logs, etc.)
func (e *JobEngine) processWorkerData(workerID string, data map[string]any) {
	// data.pipelines: worker-defined pipeline definitions
	if rawPipelines, ok := data["pipelines"]; ok {
		if pipelines, ok := rawPipelines.(map[string]any); ok {
			e.registerWorkerPipelines(workerID, pipelines)
		}
	}
}

// registerWorkerPipelines merges worker-provided pipeline definitions into the config.
// Each pipeline gets DesignedBy set to the worker ID.
func (e *JobEngine) registerWorkerPipelines(workerID string, pipelines map[string]any) {
	for id, raw := range pipelines {
		def, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		p := config.Pipeline{
			DesignedBy: workerID,
		}
		if v, ok := def["label"].(string); ok {
			p.Label = v
		}
		if v, ok := def["icon"].(string); ok {
			p.Icon = v
		}
		if v, ok := def["menu"].(string); ok {
			p.Menu = v
		}
		if v, ok := def["notification"].(string); ok {
			p.Notification = v
		}
		if types, ok := def["source_types"].([]any); ok {
			for _, t := range types {
				if s, ok := t.(string); ok {
					p.SourceTypes = append(p.SourceTypes, s)
				}
			}
		}
		if job, ok := def["job"].(map[string]any); ok {
			if v, ok := job["type"].(string); ok {
				p.Job.Type = v
			}
			if params, ok := job["params"].(map[string]any); ok {
				p.Job.Params = params
			}
		}
		if shares, ok := def["shares"].(map[string]any); ok {
			if v, ok := shares["type"].(string); ok {
				p.Shares = &config.SharesConfig{Type: v}
			}
		}
		if p.Job.Type == "" {
			p.Job.Type = id
		}

		e.cfg.Pipelines[id] = p
	}
}

// isPollingTooFast checks if the worker is polling below the minimum interval
func (e *JobEngine) isPollingTooFast(workerID string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if last, ok := e.heartbeats[workerID]; ok {
		minInterval := time.Duration(e.cfg.Service.PollIntervalMin) * time.Second
		if minInterval > 0 && time.Since(last) < minInterval {
			return true
		}
	}
	return false
}

// recordHeartbeat updates the last-seen timestamp for a worker
func (e *JobEngine) recordHeartbeat(workerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.heartbeats == nil {
		e.heartbeats = make(map[string]time.Time)
	}
	e.heartbeats[workerID] = time.Now()
}

// recordPick stores the job types offered by a worker
func (e *JobEngine) recordPick(workerID string, pick []string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.workerPick == nil {
		e.workerPick = make(map[string][]string)
	}
	e.workerPick[workerID] = pick
}

// recordCapacity stores the capacity reported by a worker
func (e *JobEngine) recordCapacity(workerID string, capacity int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.workerCap[workerID] = capacity
}

// QueuedJobCount returns the number of queued (unassigned) jobs.
func (e *JobEngine) QueuedJobCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	count := 0
	for _, job := range e.jobs {
		if job.Status == StatusQueued {
			count++
		}
	}
	return count
}

// AvailableCapacity returns total available worker capacity
// (sum of all online workers' capacity minus their running jobs).
func (e *JobEngine) AvailableCapacity() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	maxInterval := time.Duration(e.cfg.Service.PollIntervalMax) * time.Second
	if maxInterval == 0 {
		maxInterval = 30 * time.Second
	}

	total := 0
	for wid, cap := range e.workerCap {
		// Only count online workers
		if last, ok := e.heartbeats[wid]; ok && now.Sub(last) < maxInterval*2 {
			running := 0
			for _, job := range e.jobs {
				if job.WorkerID == wid && (job.Status == StatusRunning || job.Status == StatusQueued) {
					running++
				}
			}
			avail := cap - running
			if avail > 0 {
				total += avail
			}
		}
	}
	return total
}

// getWorkerSlots checks the pipe matrix and returns allowed slots + denied types
func (e *JobEngine) getWorkerSlots(workerID string, pick []string) (slots map[string]int, denied []string) {
	// Use persistent matrix if available, otherwise fall back to in-memory map
	if e.matrix != nil {
		return e.matrix.GetWorkerSlots(workerID, pick)
	}

	slots = make(map[string]int)
	denied = make([]string, 0)

	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, jobType := range pick {
		if s, ok := e.pipeMatrix[workerID]; ok {
			if n, ok := s[jobType]; ok && n > 0 {
				slots[jobType] = n
				continue
			}
		}
		denied = append(denied, jobType)
	}

	return slots, denied
}

// getWorkerCancellations returns job IDs that this worker should cancel
func (e *JobEngine) getWorkerCancellations(workerID string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var cancellations []string
	for _, job := range e.jobs {
		if job.WorkerID == workerID && job.Status == StatusCancelled {
			cancellations = append(cancellations, job.ID)
		}
	}
	return cancellations
}

// pickJobs atomically assigns queued jobs to the worker
func (e *JobEngine) pickJobs(workerID string, slots map[string]int, capacity int) []JobAssignment {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Count how many jobs this worker already has running
	running := 0
	for _, job := range e.jobs {
		if job.WorkerID == workerID && (job.Status == StatusRunning || job.Status == StatusQueued) {
			running++
		}
	}

	available := capacity - running
	if available <= 0 {
		return nil
	}

	// Count per-type running jobs for slot enforcement
	typeRunning := make(map[string]int)
	for _, job := range e.jobs {
		if job.WorkerID == workerID && (job.Status == StatusRunning) {
			typeRunning[job.Pipeline]++
		}
	}

	// Collect eligible jobs and sort by priority (higher first)
	type candidate struct {
		job *Job
		idx int
	}
	var candidates []candidate
	now := time.Now()

	for _, job := range e.jobs {
		if job.Status != StatusQueued {
			continue
		}

		// Check if this job type is in the allowed slots
		maxSlots, ok := slots[job.Pipeline]
		if !ok {
			continue
		}

		// Check slot limit for this type
		if typeRunning[job.Pipeline] >= maxSlots {
			continue
		}

		// Check if job is still valid
		if !job.ValidTill.IsZero() && job.ValidTill.Before(now) {
			job.Status = StatusExpired
			continue
		}

		// Check ETA — don't pick before scheduled time
		if !job.ETA.IsZero() && job.ETA.After(now) {
			continue
		}

		// Check dependencies — all must be completed
		if len(job.DependsOn) > 0 {
			allDone := true
			for _, depID := range job.DependsOn {
				dep, exists := e.jobs[depID]
				if !exists || dep.Status != StatusCompleted {
					allDone = false
					break
				}
			}
			if !allDone {
				continue
			}
		}

		// Check max retries
		if pipeline, ok := e.cfg.Pipelines[job.Pipeline]; ok {
			if pipeline.Job.MaxRetries > 0 && job.Retries >= pipeline.Job.MaxRetries {
				job.Status = StatusFailed
				job.Error = "max retries exceeded"
				continue
			}
		}

		candidates = append(candidates, candidate{job: job})
	}

	// Sort by priority (descending), then by creation time (ascending)
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0; j-- {
			a, b := candidates[j], candidates[j-1]
			if a.job.Priority > b.job.Priority || (a.job.Priority == b.job.Priority && a.job.CreatedAt.Before(b.job.CreatedAt)) {
				candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
			}
		}
	}

	var assignments []JobAssignment

	for _, c := range candidates {
		if len(assignments) >= available {
			break
		}
		job := c.job

		// Atomic pick
		job.Status = StatusRunning
		job.WorkerID = workerID
		job.PickedAt = now

		pipeline, _ := e.cfg.Pipelines[job.Pipeline]

		// Use pipeline ID as job type (e.g. "md-to-pdf"), not executor type
		jobType := job.Pipeline
		if pipeline.Job.Type != "" {
			jobType = pipeline.Job.Type
		}

		// Merge params: pipeline defaults + job-specific params (job wins)
		mergedParams := make(map[string]any)
		for k, v := range pipeline.Job.Params {
			mergedParams[k] = v
		}
		if jobParams, ok := job.Params.(map[string]any); ok {
			for k, v := range jobParams {
				mergedParams[k] = v
			}
		}

		assignment := JobAssignment{
			JobID: job.ID,
			Job: JobDescription{
				Type:   jobType,
				Params: mergedParams,
			},
			Timeout:   int(pipeline.Job.Timeout.Seconds()),
			ValidTill: job.ValidTill.Format(time.RFC3339),
		}

		// TODO: attach origin/destination shares from job context
		// For now, these will be populated when ephemeral shares are implemented

		assignments = append(assignments, assignment)
		typeRunning[job.Pipeline]++
	}

	return assignments
}
