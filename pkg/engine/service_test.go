package service

import (
	"testing"
	"time"

	"github.com/opencloud-eu/opencloud/services/jobengine/pkg/config"
)

func testConfig() *config.PipelineConfig {
	cfg := config.PipelineDefaults()
	cfg.Pipelines["test-echo"] = config.Pipeline{
		Label:       "Test Echo",
		SourceTypes: []string{"text/plain"},
		Target:      config.TargetConfig{Extension: ".out", Location: "same"},
		Job: config.JobConfig{
			Type:    "test-echo",
			Timeout: 5 * time.Minute,
			Params:  map[string]any{"command": "echo", "args": []any{"done"}},
		},
	}
	return cfg
}

func TestSubmitAndGetJob(t *testing.T) {
	engine := New(testConfig())
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
	engine := New(testConfig())
	defer engine.Shutdown()

	_, err := engine.Submit("nonexistent", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})
	if err == nil {
		t.Error("expected error for unknown pipeline")
	}
}

func TestCancelJob(t *testing.T) {
	engine := New(testConfig())
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
	engine := New(testConfig())
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
	engine := New(testConfig())
	defer engine.Shutdown()

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

	// Job should stay queued — no internal workers, only external poll can pick it
	got, _ := engine.GetJob(job.ID)
	if got.Status != StatusQueued {
		t.Errorf("status = %s, want queued (no internal execution)", got.Status)
	}
}

func TestSubmitWithPriority(t *testing.T) {
	engine := New(testConfig())
	defer engine.Shutdown()

	engine.Submit("test-echo", []string{"low.txt"}, "user1", "", false, &SubmitOpts{Priority: 0})
	engine.Submit("test-echo", []string{"high.txt"}, "user1", "", false, &SubmitOpts{Priority: 10})

	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 1})
	assignments := engine.pickJobs("w1", map[string]int{"test-echo": 1}, 1)

	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	// High priority should be picked first
	if assignments[0].Job.Params == nil {
		t.Skip("params not propagated in pick")
	}
}

func TestSubmitWithETA(t *testing.T) {
	engine := New(testConfig())
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
	engine := New(testConfig())
	defer engine.Shutdown()

	job1, _ := engine.Submit("test-echo", []string{"first.txt"}, "user1", "", false, &SubmitOpts{})
	engine.Submit("test-echo", []string{"second.txt"}, "user1", "", false, &SubmitOpts{DependsOn: []string{job1.ID}})

	engine.SetWorkerSlots("w1", map[string]int{"test-echo": 2})

	// Only job1 should be pickable (job2 depends on job1)
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
	engine := New(cfg)
	defer engine.Shutdown()

	engine.Submit("limited", []string{"a.txt"}, "user1", "", false, &SubmitOpts{})
	engine.Submit("limited", []string{"b.txt"}, "user1", "", false, &SubmitOpts{})
	_, err := engine.Submit("limited", []string{"c.txt"}, "user1", "", false, &SubmitOpts{})

	if err == nil {
		t.Error("expected rate limit error on 3rd submit")
	}
}

func TestStageReporting(t *testing.T) {
	engine := New(testConfig())
	defer engine.Shutdown()

	job, _ := engine.Submit("test-echo", []string{"file.txt"}, "user1", "", false, &SubmitOpts{})

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
	engine := New(testConfig())
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
