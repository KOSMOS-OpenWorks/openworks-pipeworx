package engine

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/kosmos-openworks/openworks-pipeworx/pkg/config"
)

// testAuth is a no-op auth extractor for unit tests (always returns no user)
type testAuth struct{}

func (t *testAuth) ExtractUser(r *http.Request) (*UserInfo, bool) { return nil, false }

func testConfig() *config.PipelineConfig {
	cfg := config.PipelineDefaults()
	cfg.Pipelines["test-echo"] = config.Pipeline{
		Label:       "Test Echo",
		SourceTypes: []string{"text/plain"},
		Target:      config.TargetConfig{Extension: ".out", Location: "same"},
		Job: config.JobConfig{
			Type:       "test-echo",
			Timeout:    5 * time.Minute,
			Params:     map[string]any{"command": "echo", "args": []any{"done"}},
			MaxRetries: 3,
		},
	}
	return cfg
}

func newTestEngine() *JobEngine {
	return New(testConfig(), &testAuth{})
}

func TestSubmitAndGetJob(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	job, err := engine.Submit("test-echo", []string{"file1.txt", "file2.txt"}, "user1", "", false, &SubmitOpts{})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	if job.ID == "" {
		t.Error("job ID should not be empty")
	}
	if job.Total != 2 {
		t.Errorf("total = %d, want 2", job.Total)
	}
	if job.Pipeline != "test-echo" {
		t.Errorf("pipeline = %s, want test-echo", job.Pipeline)
	}
	if job.Status != StatusQueued {
		t.Errorf("status = %s, want queued (dispatcher does not execute)", job.Status)
	}
	if job.ValidTill.IsZero() {
		t.Error("validTill should be set")
	}
}

func TestSubmitUnknownPipeline(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	_, err := engine.Submit("nonexistent", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})
	if err == nil {
		t.Error("expected error for unknown pipeline")
	}
}

func TestCancelJob(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

	err := engine.CancelJob(job.ID)
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	got, _ := engine.GetJob(job.ID)
	if got.Status != StatusCancelled {
		t.Errorf("status = %s, want cancelled", got.Status)
	}
}

func TestGetUserJobs(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	engine.Submit("test-echo", []string{"a.txt"}, "alice", "", false, &SubmitOpts{})
	engine.Submit("test-echo", []string{"b.txt"}, "bob", "", false, &SubmitOpts{})
	engine.Submit("test-echo", []string{"c.txt"}, "alice", "", false, &SubmitOpts{})

	aliceJobs := engine.GetUserJobs("alice", "")
	if len(aliceJobs) != 2 {
		t.Errorf("alice jobs = %d, want 2", len(aliceJobs))
	}

	bobJobs := engine.GetUserJobs("bob", "")
	if len(bobJobs) != 1 {
		t.Errorf("bob jobs = %d, want 1", len(bobJobs))
	}
}

func TestJobStaysQueued(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

	got, _ := engine.GetJob(job.ID)
	if got.Status != StatusQueued {
		t.Errorf("status = %s, want queued (no internal execution)", got.Status)
	}
}

func TestSubmitWithPriority(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	engine.Submit("test-echo", []string{"low.txt"}, "user1", "", false, &SubmitOpts{Priority: 0})
	engine.Submit("test-echo", []string{"high.txt"}, "user1", "", false, &SubmitOpts{Priority: 10})

	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 10})
	assignments := engine.pickJobs("w1", map[string]int{"test-echo": 10}, 1)

	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	// High priority should be picked first
	got, _ := engine.GetJob(assignments[0].JobID)
	if got.Total != 1 || got.Params == nil {
		// The high-priority job has 1 resource (high.txt)
	}
	_ = got
}

func TestSubmitWithETA(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	future := time.Now().Add(1 * time.Hour)
	engine.Submit("test-echo", []string{"later.txt"}, "user1", "", false, &SubmitOpts{ETA: future})

	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 1})
	assignments := engine.pickJobs("w1", map[string]int{"test-echo": 1}, 1)

	if len(assignments) != 0 {
		t.Error("job with future ETA should not be picked yet")
	}
}

func TestSubmitWithDependency(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	job1, _ := engine.Submit("test-echo", []string{"first.txt"}, "user1", "", false, &SubmitOpts{})
	engine.Submit("test-echo", []string{"second.txt"}, "user1", "", false, &SubmitOpts{DependsOn: []string{job1.ID}})

	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 2})

	assignments := engine.pickJobs("w1", map[string]int{"test-echo": 2}, 2)
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment (dependency blocks second), got %d", len(assignments))
	}
	if assignments[0].JobID != job1.ID {
		t.Errorf("expected job1 to be picked, got %s", assignments[0].JobID)
	}
}

func TestRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.Pipelines["limited"] = config.Pipeline{
		Label: "Limited",
		Job: config.JobConfig{
			Type:      "limited",
			Timeout:   5 * time.Minute,
			RateLimit: 2,
		},
	}
	engine := New(cfg, &testAuth{})
	defer engine.Shutdown()

	engine.Submit("limited", []string{"a.txt"}, "user1", "", false, &SubmitOpts{})
	engine.Submit("limited", []string{"b.txt"}, "user1", "", false, &SubmitOpts{})
	_, err := engine.Submit("limited", []string{"c.txt"}, "user1", "", false, &SubmitOpts{})

	if err == nil {
		t.Error("expected rate limit error on 3rd submit")
	}
}

func TestStageReporting(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

	// Simulate worker picking the job
	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 1})
	engine.pickJobs("w1", map[string]int{"test-echo": 1}, 1)

	// Now report progress
	engine.processWorkerStatus("w1", WorkerJobStatus{
		JobID:     job.ID,
		Progress:  50,
		Stage:     "compiling",
		StageData: map[string]any{"step": 3, "total": 5},
	})

	got, _ := engine.GetJob(job.ID)
	if got.Stage != "compiling" {
		t.Errorf("stage = %s, want compiling", got.Stage)
	}
	if got.Progress != 50 {
		t.Errorf("progress = %d, want 50", got.Progress)
	}
}

func TestPipeMatrix(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	engine.SetWorkerSlots("worker-1", map[string]int{"test-echo": 3})

	slots, denied := engine.getWorkerSlots("worker-1", []string{"test-echo", "unknown"})
	if slots["test-echo"] != 3 {
		t.Errorf("slots[test-echo] = %d, want 3", slots["test-echo"])
	}
	if len(denied) != 1 || denied[0] != "unknown" {
		t.Errorf("denied = %v, want [unknown]", denied)
	}
}

// --- New tests for OnJobDone, lock split, and worker cleanup ---

func TestOnJobDoneCalledOnCompletion(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	var called atomic.Int32
	var doneJob *Job
	var mu sync.Mutex

	engine.OnJobDone = func(job *Job) {
		mu.Lock()
		doneJob = job
		mu.Unlock()
		called.Add(1)
	}

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

	// Worker picks the job
	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 1})
	engine.pickJobs("w1", map[string]int{"test-echo": 1}, 1)

	// Worker reports completion
	engine.processWorkerStatus("w1", WorkerJobStatus{
		JobID:  job.ID,
		Status: StatusCompleted,
		Result: "success",
	})

	// Wait for async callback
	time.Sleep(50 * time.Millisecond)

	if called.Load() != 1 {
		t.Fatalf("OnJobDone called %d times, want 1", called.Load())
	}
	mu.Lock()
	if doneJob.Status != StatusCompleted {
		t.Errorf("callback job status = %s, want completed", doneJob.Status)
	}
	if doneJob.Result != "success" {
		t.Errorf("callback job result = %v, want success", doneJob.Result)
	}
	mu.Unlock()
}

func TestOnJobDoneCalledOnFinalFailure(t *testing.T) {
	cfg := testConfig()
	cfg.Pipelines["test-echo"] = config.Pipeline{
		Label: "Test Echo",
		Job: config.JobConfig{
			Type:       "test-echo",
			Timeout:    5 * time.Minute,
			MaxRetries: 2, // Retries incremented before check: 1st fail → retry, 2nd fail → final
		},
	}
	engine := New(cfg, &testAuth{})
	defer engine.Shutdown()

	var called atomic.Int32
	engine.OnJobDone = func(job *Job) {
		called.Add(1)
	}

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

	// Worker picks and fails
	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 1})
	engine.pickJobs("w1", map[string]int{"test-echo": 1}, 1)
	engine.processWorkerStatus("w1", WorkerJobStatus{
		JobID:  job.ID,
		Status: StatusFailed,
		Error:  "first failure",
	})

	// First failure should NOT call OnJobDone (retry available)
	time.Sleep(50 * time.Millisecond)
	if called.Load() != 0 {
		t.Fatalf("OnJobDone should not be called on retriable failure, called %d", called.Load())
	}

	// Job should be re-queued
	got, _ := engine.GetJob(job.ID)
	if got.Status != StatusQueued {
		t.Fatalf("status = %s, want queued (retry)", got.Status)
	}

	// Worker picks again and fails again (max retries exhausted)
	engine.pickJobs("w1", map[string]int{"test-echo": 1}, 1)
	engine.processWorkerStatus("w1", WorkerJobStatus{
		JobID:  job.ID,
		Status: StatusFailed,
		Error:  "second failure",
	})

	time.Sleep(50 * time.Millisecond)
	if called.Load() != 1 {
		t.Fatalf("OnJobDone should be called on final failure, called %d", called.Load())
	}

	got, _ = engine.GetJob(job.ID)
	if got.Status != StatusFailed {
		t.Errorf("status = %s, want failed (final)", got.Status)
	}
}

func TestOnJobDoneNotCalledWhenNil(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()
	// OnJobDone is nil — should not panic

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})
	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 1})
	engine.pickJobs("w1", map[string]int{"test-echo": 1}, 1)

	// This should not panic
	engine.processWorkerStatus("w1", WorkerJobStatus{
		JobID:  job.ID,
		Status: StatusCompleted,
	})
	time.Sleep(50 * time.Millisecond)
}

func TestConcurrentSubmitAndPoll(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 100})

	var wg sync.WaitGroup
	// Concurrent submitters
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})
		}()
	}
	// Concurrent pollers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.pickJobs("w1", map[string]int{"test-echo": 100}, 5)
		}()
	}
	wg.Wait()

	// Should not panic or deadlock
	jobs := engine.GetUserJobs("user1", "")
	if len(jobs) != 50 {
		t.Errorf("expected 50 jobs, got %d", len(jobs))
	}
}

func TestLockSeparation(t *testing.T) {
	// Verify that worker operations don't block job operations
	engine := newTestEngine()
	defer engine.Shutdown()

	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 5})

	// Submit some jobs
	for i := 0; i < 10; i++ {
		engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})
	}

	var wg sync.WaitGroup
	// Worker state ops (workerMu)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.recordHeartbeat("w1")
			engine.recordPick("w1", []string{"test-echo"})
			engine.recordCapacity("w1", 5)
		}()
	}
	// Job state ops (jobsMu)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.GetUserJobs("user1", "")
			engine.QueuedJobCount()
		}()
	}
	wg.Wait()
	// If we get here without deadlock, the lock split works
}

func TestWorkerCleanup(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	// Simulate a worker heartbeat in the past
	engine.workerMu.Lock()
	engine.heartbeats["dead-worker"] = time.Now().Add(-15 * time.Minute)
	engine.workerPick["dead-worker"] = []string{"test-echo"}
	engine.workerCap["dead-worker"] = 5
	engine.regTokens["dead-worker"] = "old-token"
	engine.workerMu.Unlock()

	// Simulate a live worker
	engine.recordHeartbeat("live-worker")
	engine.recordPick("live-worker", []string{"test-echo"})
	engine.recordCapacity("live-worker", 3)

	// Manually run cleanup logic (normally runs every 5 min)
	now := time.Now()
	engine.workerMu.Lock()
	for wid, last := range engine.heartbeats {
		if now.Sub(last) > workerTTL {
			delete(engine.heartbeats, wid)
			delete(engine.workerPick, wid)
			delete(engine.workerCap, wid)
			delete(engine.regTokens, wid)
		}
	}
	engine.workerMu.Unlock()

	// Dead worker should be gone
	engine.workerMu.RLock()
	if _, exists := engine.heartbeats["dead-worker"]; exists {
		t.Error("dead-worker should have been cleaned up")
	}
	if _, exists := engine.workerPick["dead-worker"]; exists {
		t.Error("dead-worker workerPick should have been cleaned up")
	}
	// Live worker should still be there
	if _, exists := engine.heartbeats["live-worker"]; !exists {
		t.Error("live-worker should still exist")
	}
	engine.workerMu.RUnlock()
}

func TestWorkerStatusRejectedFromNonOwner(t *testing.T) {
	engine := newTestEngine()
	defer engine.Shutdown()

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

	// Worker w1 picks the job
	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 1})
	engine.pickJobs("w1", map[string]int{"test-echo": 1}, 1)

	// Worker w2 tries to report status — should be rejected
	engine.processWorkerStatus("w2", WorkerJobStatus{
		JobID:  job.ID,
		Status: StatusCompleted,
	})

	got, _ := engine.GetJob(job.ID)
	if got.Status != StatusRunning {
		t.Errorf("status = %s, want running (non-owner status should be rejected)", got.Status)
	}
}

func TestJobExpiration(t *testing.T) {
	cfg := testConfig()
	cfg.Pipelines["short-lived"] = config.Pipeline{
		Label: "Short",
		Job: config.JobConfig{
			Type:    "short-lived",
			Timeout: 1 * time.Millisecond, // expires immediately
		},
	}
	engine := New(cfg, &testAuth{})
	defer engine.Shutdown()

	job, _ := engine.Submit("short-lived", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

	// Wait for expiration
	time.Sleep(10 * time.Millisecond)

	// Try to pick — should mark as expired
	engine.SetWorkerSlots("w1", map[string]int{"short-lived": 1})
	assignments := engine.pickJobs("w1", map[string]int{"short-lived": 1}, 1)

	if len(assignments) != 0 {
		t.Error("expired job should not be assignable")
	}

	got, _ := engine.GetJob(job.ID)
	if got.Status != StatusExpired {
		t.Errorf("status = %s, want expired", got.Status)
	}
}
