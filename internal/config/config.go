// Package config loads and validates declarative pipeline configuration files.
//
// Configuration is JSON (a small example):
//
//	{
//	  "name": "my-pipeline",
//	  "repo": "/path/to/repo",
//	  "concurrency": 2,
//	  "run_timeout": "30m",
//	  "triggers": { "branches": ["main", "release/*"], "tags": ["v*"] },
//	  "stages": [
//	    { "name": "build", "tasks": [{"name":"compile","shell":true,"command":"go build ./..."}] },
//	    { "name": "test", "needs": ["build"], "retry": 2,
//	      "tasks": [{"name":"unit","shell":true,"command":"go test ./..."}] },
//	    { "name": "deploy", "needs": ["test"],
//	      "approval": {"required": true, "approvers": ["alice"], "timeout": "2h"},
//	      "tasks": [{"name":"ship","shell":true,"command":"./deploy.sh"}] }
//	  ]
//	}
package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// Load reads, parses and validates a pipeline configuration file.
func Load(_ context.Context, path string) (*model.Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	cfg := &model.Config{}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := Validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks a configuration for internal consistency:
// unique stage names, valid dependency references, no dependency cycles,
// at least one task per stage and at least one trigger filter.
func Validate(cfg *model.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if cfg.Name == "" {
		return fmt.Errorf("config.name must not be empty")
	}
	if cfg.Repo == "" {
		return fmt.Errorf("config.repo must not be empty")
	}
	if len(cfg.Triggers.Branches) == 0 && len(cfg.Triggers.Tags) == 0 {
		return fmt.Errorf("config.triggers must declare at least one branch or tag pattern")
	}
	if len(cfg.Stages) == 0 {
		return fmt.Errorf("config.stages must contain at least one stage")
	}
	if cfg.Concurrency < 0 {
		return fmt.Errorf("config.concurrency must be >= 0")
	}

	names := make(map[string]int, len(cfg.Stages))
	for i, st := range cfg.Stages {
		if st.Name == "" {
			return fmt.Errorf("stages[%d]: name must not be empty", i)
		}
		if prev, ok := names[st.Name]; ok {
			return fmt.Errorf("duplicate stage name %q (stages %d and %d)", st.Name, prev, i)
		}
		names[st.Name] = i
		if len(st.Tasks) == 0 {
			return fmt.Errorf("stage %q must contain at least one task", st.Name)
		}
		for j, t := range st.Tasks {
			if t.Name == "" {
				return fmt.Errorf("stage %q tasks[%d]: name must not be empty", st.Name, j)
			}
			if t.Command == "" {
				return fmt.Errorf("stage %q task %q: command must not be empty", st.Name, t.Name)
			}
		}
		if st.Retry < 0 {
			return fmt.Errorf("stage %q retry must be >= 0", st.Name)
		}
	}

	// Validate explicit needs references and detect cycles.
	deps := make(map[string][]string, len(cfg.Stages))
	for _, st := range cfg.Stages {
		for _, dep := range st.Needs {
			if _, ok := names[dep]; !ok {
				return fmt.Errorf("stage %q needs unknown stage %q", st.Name, dep)
			}
			if dep == st.Name {
				return fmt.Errorf("stage %q cannot depend on itself", st.Name)
			}
		}
		deps[st.Name] = st.Needs
	}
	if cycle := findCycle(deps); cycle != "" {
		return fmt.Errorf("stage dependency cycle detected: %s", cycle)
	}
	return nil
}

// findCycle returns a description of a dependency cycle, or "" when the DAG is acyclic.
func findCycle(deps map[string][]string) string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(deps))
	var stack []string
	var dfs func(n string) string
	dfs = func(n string) string {
		color[n] = gray
		stack = append(stack, n)
		for _, dep := range deps[n] {
			switch color[dep] {
			case gray:
				// Found a back edge: extract the cycle from the stack.
				start := 0
				for i, s := range stack {
					if s == dep {
						start = i
						break
					}
				}
				cyc := append(append([]string{}, stack[start:]...), dep)
				out := ""
				for i, s := range cyc {
					if i > 0 {
						out += " -> "
					}
					out += s
				}
				return out
			case white:
				if c := dfs(dep); c != "" {
					return c
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return ""
	}
	for n := range deps {
		if color[n] == white {
			if c := dfs(n); c != "" {
				return c
			}
		}
	}
	return ""
}
