package engine

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"codeberg.org/kosmos-openworks/openworks-pipeworx/pkg/config"
)

// JobStatus represents the state of a job
type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
	StatusExpired   JobStatus = "expired"
)

// Job is a queued/running/completed job
type Job struct {
	ID          string    `json:"jobId"`
	Pipeline    string    `json:"pipeline"`
	Status      JobStatus `json:"status"`
	Progress    int       `json:"progress"`
	Stage       string    `json:"stage,omitempty"`
	StageData   any       `json:"stageData,omitempty"`
	Total       int       `json:"total"`
	Priority    int       `json:"priority,omitempty"`
	Error       string    `json:"error,omitempty"`
	Params      any       `json:"params,omitempty"`
	Result      any       `json:"result,omitempty"`
	DependsOn   []string  `json:"dependsOn,omitempty"`
	ETA         time.Time `json:"eta,omitempty"`
	UserID      string    `json:"userId"`
	CreatedAt   time.Time `json:"createdAt"`
	ValidTill   time.Time `json:"validTill,omitempty"`
	WorkerID    string    `json:"workerId,omitempty"`
	PickedAt    time.Time `json:"pickedAt,omitempty"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
	Retries     int       `json:"retries,omitempty"`
}

// JobEngine is the core dispatcher service.
// It does NOT execute jobs — workers pick them via the poll endpoint.
type JobEngine struct {
	cfg         *config.PipelineConfig
	auth        AuthExtractor
	jobs        map[string]*Job
	mu          sync.RWMutex
	stopCleanup chan struct{}

	// Worker polling state
	heartbeats map[string]time.Time       // workerID → last poll time
	workerPick map[string][]string        // workerID → offered job types
	pipeMatrix map[string]map[string]int   // workerID → { jobType → slots }
	matrix     *PipeMatrix                 // persistent matrix (if loaded from file)
	regTokens  map[string]string           // workerID → regToken (pipeline registration receipt)

	// Optional backend store (XIS provides this, OpenCloud does not)
	storeProvider StoreProvider
}

// cleanupInterval removes completed/failed jobs older than 1 hour
const jobRetention = 1 * time.Hour

// New creates a new JobEngine (pure dispatcher, no internal workers)
func New(cfg *config.PipelineConfig, auth AuthExtractor) *JobEngine {
	e := &JobEngine{
		cfg:         cfg,
		auth:        auth,
		jobs:        make(map[string]*Job),
		stopCleanup: make(chan struct{}),
		heartbeats:  make(map[string]time.Time),
		workerPick:  make(map[string][]string),
		pipeMatrix:  make(map[string]map[string]int),
		regTokens:   make(map[string]string),
	}

	// start cleanup goroutine
	go e.cleanupLoop()

	return e
}

// Submit creates and queues a new job. The job sits in the queue
// until a worker picks it via the poll endpoint.
// SubmitOpts holds optional fields for job submission
type SubmitOpts struct {
	Params    any       `json:"params,omitempty"`
	Priority  int       `json:"priority,omitempty"`
	ETA       time.Time `json:"eta,omitempty"`
	DependsOn []string  `json:"dependsOn,omitempty"`
}

func (e *JobEngine) Submit(pipelineID string, resources []string, userID string, targetPath string, createDirs bool, opts *SubmitOpts) (*Job, error) {
	pipeline, ok := e.cfg.Pipelines[pipelineID]
	if !ok {
		return nil, fmt.Errorf("unknown pipeline: %s", pipelineID)
	}

	// Calculate validTill from pipeline job timeout
	var validTill time.Time
	if pipeline.Job.Timeout > 0 {
		validTill = time.Now().Add(pipeline.Job.Timeout)
	} else if pipeline.Executor.Timeout > 0 {
		// legacy fallback
		validTill = time.Now().Add(pipeline.Executor.Timeout)
	} else {
		validTill = time.Now().Add(1 * time.Hour) // default 1h
	}

	if opts == nil {
		opts = &SubmitOpts{}
	}

	// Rate limit: check concurrent jobs for this pipeline
	if pipeline.Job.RateLimit > 0 {
		e.mu.RLock()
		active := 0
		for _, j := range e.jobs {
			if j.Pipeline == pipelineID && (j.Status == StatusQueued || j.Status == StatusRunning) {
				active++
			}
		}
		e.mu.RUnlock()
		if active >= pipeline.Job.RateLimit {
			return nil, fmt.Errorf("rate limit exceeded: %d/%d active jobs for pipeline %s", active, pipeline.Job.RateLimit, pipelineID)
		}
	}

	job := &Job{
		ID:        uuid.New().String(),
		Pipeline:  pipelineID,
		Status:    StatusQueued,
		Total:     len(resources),
		Params:    opts.Params,
		Priority:  opts.Priority,
		ETA:       opts.ETA,
		DependsOn: opts.DependsOn,
		UserID:    userID,
		CreatedAt: time.Now(),
		ValidTill: validTill,
	}

	e.mu.Lock()
	e.jobs[job.ID] = job
	e.mu.Unlock()

	return job, nil
}

// GetJob returns a job by ID
func (e *JobEngine) GetJob(jobID string) (*Job, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	job, ok := e.jobs[jobID]
	return job, ok
}

// GetUserJobs returns all jobs for a user, optionally filtered by status
func (e *JobEngine) GetUserJobs(userID string, statusFilter JobStatus) []*Job {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []*Job
	for _, job := range e.jobs {
		if userID != "" && job.UserID != userID {
			continue
		}
		if statusFilter != "" && job.Status != statusFilter {
			continue
		}
		result = append(result, job)
	}
	return result
}

// CancelJob cancels a queued or running job
func (e *JobEngine) CancelJob(jobID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	job, ok := e.jobs[jobID]
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}
	job.Status = StatusCancelled
	return nil
}

// Pipelines returns all registered pipelines
func (e *JobEngine) Pipelines() map[string]config.Pipeline {
	return e.cfg.Pipelines
}

// SetPipeMatrix sets the capability matrix for workers
func (e *JobEngine) SetPipeMatrix(matrix map[string]map[string]int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pipeMatrix = matrix
}

// SetWorkerSlots sets the slots for a single worker in the pipe matrix
func (e *JobEngine) SetWorkerSlots(workerID string, slots map[string]int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pipeMatrix[workerID] = slots
}

// LoadMatrix loads the pipe matrix from a YAML file and applies it
func (e *JobEngine) LoadMatrix(path string) error {
	m, err := LoadPipeMatrix(path)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.matrix = m
	e.pipeMatrix = m.ToEngineFormat()
	e.mu.Unlock()
	return nil
}

// Shutdown stops the cleanup goroutine
func (e *JobEngine) Shutdown() {
	close(e.stopCleanup)
}

func (e *JobEngine) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.mu.Lock()
			now := time.Now()
			for id, job := range e.jobs {
				// Clean up finished jobs 1h after completion
				if (job.Status == StatusCompleted || job.Status == StatusFailed ||
					job.Status == StatusCancelled || job.Status == StatusExpired) &&
					!job.CompletedAt.IsZero() && now.Sub(job.CompletedAt) > jobRetention {
					delete(e.jobs, id)
				}
				// Expire jobs past validTill
				if job.Status == StatusQueued && !job.ValidTill.IsZero() && now.After(job.ValidTill) {
					job.Status = StatusExpired
				}
			}
			e.mu.Unlock()
		case <-e.stopCleanup:
			return
		}
	}
}
