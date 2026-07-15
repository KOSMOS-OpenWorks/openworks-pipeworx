package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// PipelineConfig is the pipeline/executor configuration (loaded from YAML files)
type PipelineConfig struct {
	Service   PipelineServiceConfig `yaml:"service"`
	Pipelines map[string]Pipeline   `yaml:"pipelines"`
}

// PipelineServiceConfig holds runtime settings for the pipeline engine
type PipelineServiceConfig struct {
	MaxWorkers      int      `yaml:"max_workers"`
	QueueSize       int      `yaml:"queue_size"`
	TempDir         string   `yaml:"temp_dir"`
	PipelineDirs    []string `yaml:"pipeline_dirs"`
	PollIntervalMin int      `yaml:"poll_interval_min"` // seconds, minimum poll frequency
	PollIntervalMax int      `yaml:"poll_interval_max"` // seconds, maximum poll frequency (heartbeat timeout)
}

// SharesConfig defines how the UI handles file sharing for a pipeline.
// Type controls what gets shared and with what permissions:
//   srcFile    — source file, read-only
//   srcDir     — source folder, read-only
//   parentDir  — parent folder of source, read+write (result goes next to source)
//   sameFile   — source file, read+write (in-place conversion)
//   sameDir    — source folder, read+write (batch processing)
//   srcDstFile — source + separate destination (needs picker)
//   srcDstDir  — source folder + destination folder (needs picker)
type SharesConfig struct {
	Type string `yaml:"type" json:"type"` // srcFile | srcDir | parentDir | sameFile | sameDir | srcDstFile | srcDstDir
}

// Pipeline defines a conversion/processing pipeline
type Pipeline struct {
	Label       string        `yaml:"label"`
	Icon        string        `yaml:"icon"`
	SourceTypes []string      `yaml:"source_types"`
	Target      TargetConfig  `yaml:"target"`
	Menu        string        `yaml:"menu"`         // "context" | "index" | "back"
	Dialog      *DialogSpec   `yaml:"dialog"`        // optional user input dialog
	Shares      *SharesConfig `yaml:"shares"`        // origin/destination share behavior
	ReadOnly    bool          `yaml:"read_only" json:"readOnly,omitempty"` // true = only reads source, no writeback — no share permission needed
	Job         JobConfig     `yaml:"job"`           // opaque job description for workers
	Notification string      `yaml:"notification"`  // "toast" | "none"
	DesignedBy  string        `yaml:"designed_by" json:"designedBy,omitempty"` // worker ID or "" for internal

	// Deprecated: kept for backward compatibility with existing YAMLs.
	// Use Job.Params instead. Will be migrated on load.
	Executor            ExecutorConfig    `yaml:"executor"`
	Batch               bool              `yaml:"batch"`
	UserChoosableTarget bool              `yaml:"user_choosable_target"`
	Options             map[string]string `yaml:"options"`
}

// JobConfig is the opaque job description passed to workers.
// The engine only reads Type and Timeout. Params is passed through uninterpreted.
type JobConfig struct {
	Type         string         `yaml:"type" json:"type"`
	Timeout      time.Duration  `yaml:"timeout" json:"timeout"`
	Params       map[string]any `yaml:"params" json:"params"`
	RateLimit    int            `yaml:"rate_limit" json:"rateLimit,omitempty"`       // max concurrent jobs for this pipeline (0=unlimited)
	MaxRetries   int            `yaml:"max_retries" json:"maxRetries,omitempty"`     // max re-picks on failure (0=no retry)
}

// DialogSpec defines an optional user input dialog shown before job submission
type DialogSpec struct {
	Title  string        `yaml:"title" json:"title"`
	Fields []DialogField `yaml:"fields" json:"fields"`
}

// DialogField is a single input field in a dialog
type DialogField struct {
	Key      string   `yaml:"key" json:"key"`
	Type     string   `yaml:"type" json:"type"`         // "text" | "select" | "checkbox"
	Label    string   `yaml:"label" json:"label"`
	Default  any      `yaml:"default" json:"default"`
	Options  []string `yaml:"options,omitempty" json:"options,omitempty"`
	Multiline bool   `yaml:"multiline,omitempty" json:"multiline,omitempty"`
}

// TargetConfig defines where the result goes
type TargetConfig struct {
	Extension  string `yaml:"extension"`
	Location   string `yaml:"location"`
	CreateDirs bool   `yaml:"create_dirs"`
}

// ExecutorConfig is deprecated — use JobConfig instead.
// Kept for backward compatibility with existing pipeline YAMLs.
type ExecutorConfig struct {
	Type        string        `yaml:"type"`
	Command     string        `yaml:"command"`
	Args        []string      `yaml:"args"`
	URL         string        `yaml:"url"`
	Method      string        `yaml:"method"`
	UploadField string        `yaml:"upload_field"`
	Path        string        `yaml:"path"`
	Timeout     time.Duration `yaml:"timeout"`
}

// MigrateExecutorToJob converts a legacy executor block into a JobConfig.
// Called after YAML loading to ensure all pipelines use the new format.
func (p *Pipeline) MigrateExecutorToJob() {
	if p.Job.Type != "" {
		return // already has job config
	}
	if p.Executor.Type == "" {
		return // nothing to migrate
	}

	p.Job.Type = p.Executor.Type
	p.Job.Timeout = p.Executor.Timeout
	p.Job.Params = map[string]any{}

	if p.Executor.Command != "" {
		p.Job.Params["command"] = p.Executor.Command
	}
	if len(p.Executor.Args) > 0 {
		p.Job.Params["args"] = p.Executor.Args
	}
	if p.Executor.URL != "" {
		p.Job.Params["url"] = p.Executor.URL
	}
	if p.Executor.Method != "" {
		p.Job.Params["method"] = p.Executor.Method
	}
	if p.Executor.UploadField != "" {
		p.Job.Params["upload_field"] = p.Executor.UploadField
	}
	if p.Executor.Path != "" {
		p.Job.Params["path"] = p.Executor.Path
	}

	// merge legacy options into params
	for k, v := range p.Options {
		p.Job.Params[k] = v
	}
}

// PipelineDefaults returns a PipelineConfig with sane defaults
func PipelineDefaults() *PipelineConfig {
	return &PipelineConfig{
		Service: PipelineServiceConfig{
			MaxWorkers:      4,
			QueueSize:       100,
			TempDir:         "/tmp/jobengine",
			PipelineDirs:    []string{"/etc/opencloud/jobs/pipelines.d"},
			PollIntervalMin: 2,
			PollIntervalMax: 30,
		},
		Pipelines: make(map[string]Pipeline),
	}
}

// LoadPipelineConfig reads the main config file and scans pipeline dirs
func LoadPipelineConfig(path string) (*PipelineConfig, error) {
	cfg := PipelineDefaults()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("reading config %s: %w", path, err)
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("parsing config %s: %w", path, err)
			}
		}
	}

	if cfg.Pipelines == nil {
		cfg.Pipelines = make(map[string]Pipeline)
	}

	for _, dir := range cfg.Service.PipelineDirs {
		if err := cfg.LoadPipelineDir(dir); err != nil {
			continue
		}
	}

	// migrate legacy executor blocks to job config
	cfg.migratePipelines()

	return cfg, nil
}

func (c *PipelineConfig) LoadPipelineDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}

		var addon struct {
			Pipelines map[string]Pipeline `yaml:"pipelines"`
		}
		if err := yaml.Unmarshal(data, &addon); err != nil {
			continue
		}

		for id, p := range addon.Pipelines {
			c.Pipelines[id] = p
		}
	}

	return nil
}

func (c *PipelineConfig) migratePipelines() {
	for id, p := range c.Pipelines {
		p.MigrateExecutorToJob()
		c.Pipelines[id] = p
	}
}
