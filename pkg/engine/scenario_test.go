package engine

import (
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/kosmos-openworks/openworks-pipeworx/pkg/config"
)

// --- Realistic scenario tests ---
// These simulate the worker-engine interaction as it happens in production:
// submit → poll (pickJobs) → status report → completion/failure

func scenarioConfig() *config.PipelineConfig {
	cfg := config.PipelineDefaults()
	cfg.Service.PollIntervalMin = 0 // no rate limit in tests
	cfg.Service.PollIntervalMax = 5
	cfg.Pipelines["convert"] = config.Pipeline{
		Label: "PDF Convert",
		Job: config.JobConfig{
			Type:       "convert-pdf",
			Timeout:    1 * time.Minute,
			MaxRetries: 2,
		},
	}
	cfg.Pipelines["thumbnail"] = config.Pipeline{
		Label: "Thumbnail",
		Job: config.JobConfig{
			Type:    "gen-thumb",
			Timeout: 30 * time.Second,
		},
	}
	return cfg
}

func scenarioEngine() *JobEngine {
	return New(scenarioConfig(), &testAuth{})
}

// TestWorkerPicksAndCompletes: happy path — submit, pick, progress, complete
func TestWorkerPicksAndCompletes(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	var callbackCalled atomic.Int32
	e.OnJobDone = func(j *Job) { callbackCalled.Add(1) }

	// User submits a job
	job, err := e.Submit("convert", []string{"doc.pdf"}, "alice", "/output", true, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Worker registers + picks
	e.SetWorkerSlots("worker-1", map[string]int{"convert": 3})
	e.recordHeartbeat("worker-1")
	assignments := e.pickJobs("worker-1", map[string]int{"convert": 3}, 3)

	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	if assignments[0].JobID != job.ID {
		t.Errorf("wrong job assigned: %s", assignments[0].JobID)
	}
	if assignments[0].Job.Type != "convert-pdf" {
		t.Errorf("job type = %s, want convert-pdf", assignments[0].Job.Type)
	}

	// Job should now be running
	got, _ := e.GetJob(job.ID)
	if got.Status != StatusRunning {
		t.Fatalf("status = %s, want running", got.Status)
	}
	if got.WorkerID != "worker-1" {
		t.Errorf("workerID = %s, want worker-1", got.WorkerID)
	}

	// Worker reports progress
	e.processWorkerStatus("worker-1", WorkerJobStatus{
		JobID:    job.ID,
		Progress: 50,
		Stage:    "converting",
	})

	got, _ = e.GetJob(job.ID)
	if got.Progress != 50 || got.Stage != "converting" {
		t.Errorf("progress=%d stage=%s, want 50/converting", got.Progress, got.Stage)
	}

	// Worker reports completion
	e.processWorkerStatus("worker-1", WorkerJobStatus{
		JobID:  job.ID,
		Status: StatusCompleted,
		Result: map[string]any{"output": "/output/doc.out"},
	})

	time.Sleep(50 * time.Millisecond)

	got, _ = e.GetJob(job.ID)
	if got.Status != StatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}
	if got.Progress != 100 {
		t.Errorf("progress = %d, want 100", got.Progress)
	}
	if callbackCalled.Load() != 1 {
		t.Errorf("OnJobDone called %d times, want 1", callbackCalled.Load())
	}
}

// TestWorkerFailsAndRetries: worker fails, engine retries, then fails final
func TestWorkerFailsAndRetries(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	var doneCount atomic.Int32
	e.OnJobDone = func(j *Job) { doneCount.Add(1) }

	job, _ := e.Submit("convert", []string{"bad.pdf"}, "alice", "", false, nil)
	e.SetWorkerSlots("worker-1", map[string]int{"convert": 2})

	// Attempt 1: pick + fail
	e.pickJobs("worker-1", map[string]int{"convert": 2}, 2)
	e.processWorkerStatus("worker-1", WorkerJobStatus{
		JobID:  job.ID,
		Status: StatusFailed,
		Error:  "corrupted file",
	})

	got, _ := e.GetJob(job.ID)
	if got.Status != StatusQueued {
		t.Fatalf("after 1st fail: status = %s, want queued (retry)", got.Status)
	}
	if got.Retries != 1 {
		t.Errorf("retries = %d, want 1", got.Retries)
	}
	if got.WorkerID != "" {
		t.Errorf("workerID should be cleared after retry, got %s", got.WorkerID)
	}

	// Attempt 2: pick + fail again (max retries = 2, so this is final)
	e.pickJobs("worker-1", map[string]int{"convert": 2}, 2)
	e.processWorkerStatus("worker-1", WorkerJobStatus{
		JobID:  job.ID,
		Status: StatusFailed,
		Error:  "still corrupted",
	})

	time.Sleep(50 * time.Millisecond)

	got, _ = e.GetJob(job.ID)
	if got.Status != StatusFailed {
		t.Fatalf("after 2nd fail: status = %s, want failed (final)", got.Status)
	}
	if got.Retries != 2 {
		t.Errorf("retries = %d, want 2", got.Retries)
	}
	if doneCount.Load() != 1 {
		t.Errorf("OnJobDone should fire once on final failure, fired %d", doneCount.Load())
	}
}

// TestWorkerStopsPolling: worker goes silent, heartbeat expires
func TestWorkerStopsPolling(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	// Worker registers
	e.recordHeartbeat("flaky-worker")
	e.recordPick("flaky-worker", []string{"convert"})
	e.recordCapacity("flaky-worker", 5)

	// Simulate stale heartbeat
	e.workerMu.Lock()
	e.heartbeats["flaky-worker"] = time.Now().Add(-15 * time.Minute)
	e.workerMu.Unlock()

	// Run cleanup
	now := time.Now()
	e.workerMu.Lock()
	for wid, last := range e.heartbeats {
		if now.Sub(last) > workerTTL {
			delete(e.heartbeats, wid)
			delete(e.workerPick, wid)
			delete(e.workerCap, wid)
			delete(e.regTokens, wid)
		}
	}
	e.workerMu.Unlock()

	// Worker should be gone from transient state
	e.workerMu.RLock()
	_, exists := e.heartbeats["flaky-worker"]
	e.workerMu.RUnlock()
	if exists {
		t.Error("flaky-worker should have been cleaned up")
	}
}

// TestCapacityDistribution: capacity > 1 should allow multiple jobs
func TestCapacityDistribution(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	// Submit 5 jobs
	for i := 0; i < 5; i++ {
		e.Submit("convert", []string{"file.pdf"}, "alice", "", false, nil)
	}

	// Worker with capacity 3 should get 3 jobs
	e.SetWorkerSlots("big-worker", map[string]int{"convert": 10})
	assignments := e.pickJobs("big-worker", map[string]int{"convert": 10}, 3)

	if len(assignments) != 3 {
		t.Errorf("expected 3 assignments for capacity 3, got %d", len(assignments))
	}

	// Second poll: capacity still 3, but already has 3 running → 0 new
	assignments2 := e.pickJobs("big-worker", map[string]int{"convert": 10}, 3)
	if len(assignments2) != 0 {
		t.Errorf("expected 0 new assignments (at capacity), got %d", len(assignments2))
	}

	// Complete one job → next poll should get 1 more
	e.processWorkerStatus("big-worker", WorkerJobStatus{
		JobID:  assignments[0].JobID,
		Status: StatusCompleted,
	})

	assignments3 := e.pickJobs("big-worker", map[string]int{"convert": 10}, 3)
	if len(assignments3) != 1 {
		t.Errorf("expected 1 new assignment after completing 1, got %d", len(assignments3))
	}
}

// TestMultipleWorkersFairDistribution: two workers share jobs
func TestMultipleWorkersFairDistribution(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	// Submit 4 jobs
	for i := 0; i < 4; i++ {
		e.Submit("convert", []string{"file.pdf"}, "alice", "", false, nil)
	}

	e.SetWorkerSlots("w1", map[string]int{"convert": 5})
	e.SetWorkerSlots("w2", map[string]int{"convert": 5})

	// Worker 1 picks 2
	a1 := e.pickJobs("w1", map[string]int{"convert": 5}, 2)
	if len(a1) != 2 {
		t.Errorf("w1: expected 2 assignments, got %d", len(a1))
	}

	// Worker 2 picks remaining 2
	a2 := e.pickJobs("w2", map[string]int{"convert": 5}, 2)
	if len(a2) != 2 {
		t.Errorf("w2: expected 2 assignments, got %d", len(a2))
	}

	// No more jobs
	a3 := e.pickJobs("w1", map[string]int{"convert": 5}, 2)
	if len(a3) != 0 {
		t.Errorf("expected 0 assignments (queue empty), got %d", len(a3))
	}
}

// TestSlotLimitEnforced: worker slot limit per job type is respected
func TestSlotLimitEnforced(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	// Submit 5 convert jobs
	for i := 0; i < 5; i++ {
		e.Submit("convert", []string{"file.pdf"}, "alice", "", false, nil)
	}

	// Worker has capacity 5 but slot limit 2 for convert-pdf
	e.SetWorkerSlots("w1", map[string]int{"convert": 2})
	assignments := e.pickJobs("w1", map[string]int{"convert": 2}, 5)

	if len(assignments) != 2 {
		t.Errorf("expected 2 assignments (slot limit), got %d", len(assignments))
	}
}

// TestMultiJobTypeWorker: worker handles multiple job types
func TestMultiJobTypeWorker(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	e.Submit("convert", []string{"doc.pdf"}, "alice", "", false, nil)
	e.Submit("thumbnail", []string{"img.jpg"}, "alice", "", false, nil)

	slots := map[string]int{"convert": 2, "thumbnail": 2}
	e.SetWorkerSlots("multi-worker", slots)
	assignments := e.pickJobs("multi-worker", slots, 5)

	if len(assignments) != 2 {
		t.Fatalf("expected 2 assignments (1 convert + 1 thumb), got %d", len(assignments))
	}

	types := map[string]bool{}
	for _, a := range assignments {
		types[a.Job.Type] = true
	}
	if !types["convert-pdf"] || !types["gen-thumb"] {
		t.Errorf("expected both job types, got %v", types)
	}
}

// TestServerRestart: simulate engine restart — jobs lost, workers re-register
func TestServerRestart(t *testing.T) {
	e1 := scenarioEngine()
	e1.SetWorkerSlots("w1", map[string]int{"convert": 3})

	// Submit and pick a job
	job, _ := e1.Submit("convert", []string{"file.pdf"}, "alice", "", false, nil)
	e1.pickJobs("w1", map[string]int{"convert": 3}, 3)

	got, _ := e1.GetJob(job.ID)
	if got.Status != StatusRunning {
		t.Fatalf("pre-restart: status = %s, want running", got.Status)
	}

	// Simulate restart: shutdown old engine, create new one
	e1.Shutdown()

	e2 := scenarioEngine()
	defer e2.Shutdown()

	// Old job is gone (in-memory only)
	_, found := e2.GetJob(job.ID)
	if found {
		t.Error("job should not exist after restart (in-memory only)")
	}

	// Worker re-registers and gets new work
	e2.SetWorkerSlots("w1", map[string]int{"convert": 3})
	newJob, _ := e2.Submit("convert", []string{"new-file.pdf"}, "alice", "", false, nil)
	assignments := e2.pickJobs("w1", map[string]int{"convert": 3}, 3)

	if len(assignments) != 1 || assignments[0].JobID != newJob.ID {
		t.Errorf("worker should pick new job after restart")
	}
}

// TestQueuedJobCountAndCapacity: verify stats functions
func TestQueuedJobCountAndCapacity(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	for i := 0; i < 5; i++ {
		e.Submit("convert", []string{"file.pdf"}, "alice", "", false, nil)
	}

	if c := e.QueuedJobCount(); c != 5 {
		t.Errorf("QueuedJobCount = %d, want 5", c)
	}

	// Register worker with capacity 3
	e.SetWorkerSlots("w1", map[string]int{"convert": 10})
	e.recordHeartbeat("w1")
	e.recordCapacity("w1", 3)

	if cap := e.AvailableCapacity(); cap != 3 {
		t.Errorf("AvailableCapacity = %d, want 3", cap)
	}

	// Pick 2 jobs
	e.pickJobs("w1", map[string]int{"convert": 10}, 2)

	if c := e.QueuedJobCount(); c != 3 {
		t.Errorf("QueuedJobCount after pick = %d, want 3", c)
	}

	// Available capacity should be 1 (3 - 2 running)
	if cap := e.AvailableCapacity(); cap != 1 {
		t.Errorf("AvailableCapacity after pick = %d, want 1", cap)
	}
}

// TestCancellationDelivery: cancelled jobs are delivered to worker on next poll
func TestCancellationDelivery(t *testing.T) {
	e := scenarioEngine()
	defer e.Shutdown()

	job, _ := e.Submit("convert", []string{"file.pdf"}, "alice", "", false, nil)
	e.SetWorkerSlots("w1", map[string]int{"convert": 3})
	e.pickJobs("w1", map[string]int{"convert": 3}, 1)

	// Cancel the running job
	e.CancelJob(job.ID)

	// Worker should receive cancellation on next poll
	cancellations := e.getWorkerCancellations("w1")
	if len(cancellations) != 1 || cancellations[0] != job.ID {
		t.Errorf("expected cancellation for %s, got %v", job.ID, cancellations)
	}
}
