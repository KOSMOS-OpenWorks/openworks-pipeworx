package engine

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"
)

// PipeMatrix is the capability assignment matrix.
// Rows: pipeline/job types. Columns: worker user IDs. Values: max slots.
type PipeMatrix struct {
	// Workers maps workerID → { jobType → slots }
	Workers map[string]map[string]int `yaml:"workers" json:"workers"`

	mu       sync.RWMutex `yaml:"-" json:"-"`
	filePath string       `yaml:"-" json:"-"`
}

// NewPipeMatrix creates an empty matrix
func NewPipeMatrix() *PipeMatrix {
	return &PipeMatrix{
		Workers: make(map[string]map[string]int),
	}
}

// LoadPipeMatrix loads the matrix from a YAML file
func LoadPipeMatrix(path string) (*PipeMatrix, error) {
	m := NewPipeMatrix()
	m.filePath = path

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil // empty matrix is valid
		}
		return nil, fmt.Errorf("reading pipe matrix %s: %w", path, err)
	}

	if err := yaml.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("parsing pipe matrix %s: %w", path, err)
	}

	if m.Workers == nil {
		m.Workers = make(map[string]map[string]int)
	}

	return m, nil
}

// Save persists the matrix to its YAML file
func (m *PipeMatrix) Save() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.filePath == "" {
		return fmt.Errorf("no file path set for pipe matrix")
	}

	data, err := yaml.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshaling pipe matrix: %w", err)
	}

	return os.WriteFile(m.filePath, data, 0644)
}

// GetWorkerSlots returns the allowed types and slots for a worker
func (m *PipeMatrix) GetWorkerSlots(workerID string, pick []string) (slots map[string]int, denied []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	slots = make(map[string]int)
	denied = make([]string, 0)

	workerSlots, exists := m.Workers[workerID]
	for _, jobType := range pick {
		if exists {
			if n, ok := workerSlots[jobType]; ok && n > 0 {
				slots[jobType] = n
				continue
			}
		}
		denied = append(denied, jobType)
	}

	return slots, denied
}

// SetWorkerSlots sets all slots for a worker (replaces existing)
func (m *PipeMatrix) SetWorkerSlots(workerID string, slots map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Workers[workerID] = slots
}

// RemoveWorker removes a worker from the matrix
func (m *PipeMatrix) RemoveWorker(workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.Workers, workerID)
}

// ToEngineFormat converts to the format used by JobEngine.pipeMatrix
func (m *PipeMatrix) ToEngineFormat() map[string]map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]map[string]int, len(m.Workers))
	for wid, slots := range m.Workers {
		copy := make(map[string]int, len(slots))
		for k, v := range slots {
			copy[k] = v
		}
		result[wid] = copy
	}
	return result
}

// --- Admin API handlers ---

// RegisterMatrixRoutes adds the admin API routes for the pipe matrix
func (e *JobEngine) RegisterMatrixRoutes(r chi.Router) {
	r.Route("/api/v0/jobs/matrix", func(r chi.Router) {
		r.Get("/", e.handleGetMatrix)
		r.Put("/", e.handleSetMatrix)
		r.Get("/workers/{workerID}", e.handleGetWorkerSlots)
		r.Put("/workers/{workerID}", e.handleSetWorkerSlots)
		r.Delete("/workers/{workerID}", e.handleDeleteWorker)
	})
}

func (e *JobEngine) handleGetMatrix(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin required"})
		return
	}

	e.workerMu.RLock()
	matrix := e.pipeMatrix
	e.workerMu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]any{"workers": matrix})
}

func (e *JobEngine) handleSetMatrix(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin required"})
		return
	}

	var req struct {
		Workers map[string]map[string]int `json:"workers"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	e.workerMu.Lock()
	e.pipeMatrix = req.Workers
	e.workerMu.Unlock()

	// Persist
	if e.matrix != nil {
		e.matrix.mu.Lock()
		e.matrix.Workers = req.Workers
		e.matrix.mu.Unlock()
		if err := e.matrix.Save(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (e *JobEngine) handleGetWorkerSlots(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin required"})
		return
	}

	workerID := chi.URLParam(r, "workerID")

	e.workerMu.RLock()
	slots, ok := e.pipeMatrix[workerID]
	e.workerMu.RUnlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "worker not found in matrix"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"workerID": workerID, "slots": slots})
}

func (e *JobEngine) handleSetWorkerSlots(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin required"})
		return
	}

	workerID := chi.URLParam(r, "workerID")

	var req struct {
		Slots map[string]int `json:"slots"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request"})
		return
	}

	e.workerMu.Lock()
	e.pipeMatrix[workerID] = req.Slots
	e.workerMu.Unlock()

	// Persist
	if e.matrix != nil {
		e.matrix.SetWorkerSlots(workerID, req.Slots)
		e.matrix.Save()
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (e *JobEngine) handleDeleteWorker(w http.ResponseWriter, r *http.Request) {
	if !e.isAdmin(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "admin required"})
		return
	}

	workerID := chi.URLParam(r, "workerID")

	e.workerMu.Lock()
	delete(e.pipeMatrix, workerID)
	e.workerMu.Unlock()

	// Persist
	if e.matrix != nil {
		e.matrix.RemoveWorker(workerID)
		e.matrix.Save()
	}

	w.WriteHeader(http.StatusNoContent)
}
