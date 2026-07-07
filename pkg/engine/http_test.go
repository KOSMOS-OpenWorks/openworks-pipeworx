package engine

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"codeberg.org/kosmos-openworks/openworks-pipeworx/pkg/config"
)

func setupTestEngine() (*JobEngine, *chi.Mux) {
	cfg := config.PipelineDefaults()
	cfg.Pipelines["test-echo"] = config.Pipeline{
		Label:       "Test Echo",
		SourceTypes: []string{"text/plain"},
		Target:      config.TargetConfig{Extension: ".out", Location: "same"},
		Job: config.JobConfig{
			Type:    "test-echo",
			Timeout: 5 * time.Minute,
			Params:  map[string]any{"command": "echo"},
		},
	}
	engine := New(cfg)
	r := chi.NewRouter()
	engine.RegisterRoutes(r)
	return engine, r
}

func TestGetPipelinesAPI(t *testing.T) {
	engine, r := setupTestEngine()
	defer engine.Shutdown()

	req := httptest.NewRequest("GET", "/api/v0/jobs/pipelines", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp PipelinesResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Pipelines) != 1 {
		t.Errorf("pipelines = %d, want 1", len(resp.Pipelines))
	}
	if resp.Pipelines[0].ID != "test-echo" {
		t.Errorf("id = %s, want test-echo", resp.Pipelines[0].ID)
	}
	if resp.Pipelines[0].JobType != "test-echo" {
		t.Errorf("jobType = %s, want test-echo", resp.Pipelines[0].JobType)
	}
}

func TestSubmitJobNoAuth(t *testing.T) {
	engine, r := setupTestEngine()
	defer engine.Shutdown()

	body, _ := json.Marshal(SubmitRequest{Pipeline: "test-echo", Resources: []string{"file.txt"}})
	req := httptest.NewRequest("POST", "/api/v0/jobs", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestSubmitJobInvalidPipelineID(t *testing.T) {
	engine, r := setupTestEngine()
	defer engine.Shutdown()

	body, _ := json.Marshal(SubmitRequest{Pipeline: "../../etc/passwd", Resources: []string{"file.txt"}})
	req := httptest.NewRequest("POST", "/api/v0/jobs", bytes.NewReader(body))
	req.Header.Set("X-User-Id", "user1")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestSubmitJobPathTraversal(t *testing.T) {
	engine, r := setupTestEngine()
	defer engine.Shutdown()

	body, _ := json.Marshal(SubmitRequest{
		Pipeline:   "test-echo",
		Resources:  []string{"file.txt"},
		TargetPath: "../../../etc/",
	})
	req := httptest.NewRequest("POST", "/api/v0/jobs", bytes.NewReader(body))
	req.Header.Set("X-User-Id", "user1")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for path traversal", w.Code)
	}
}

func TestGetJobNotFound(t *testing.T) {
	engine, r := setupTestEngine()
	defer engine.Shutdown()

	req := httptest.NewRequest("GET", "/api/v0/jobs/nonexistent", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}
