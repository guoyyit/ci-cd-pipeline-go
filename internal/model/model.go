// Package model defines the core domain types of the local CI/CD pipeline service:
// Git trigger events, declarative pipeline configuration, run/stage/task states
// and approval records.
package model

import (
	"encoding/json"
	"errors"
	"time"
)

// Status is the lifecycle status of a run, stage or task.
type Status string

const (
	StatusPending  Status = "pending"  // created but not yet started (queued / not yet scheduled)
	StatusWaiting  Status = "waiting"  // blocked on upstream stages or a manual approval gate
	StatusRunning  Status = "running"  // executing
	StatusSuccess  Status = "success"  // finished successfully
	StatusFailed   Status = "failed"   // finished with failure
	StatusCanceled Status = "canceled" // canceled by user, timeout or shutdown
	StatusSkipped  Status = "skipped"  // not executed because an upstream stage failed/canceled
)

// IsTerminal reports whether the status is a final state.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSuccess, StatusFailed, StatusCanceled, StatusSkipped:
		return true
	}
	return false
}

// Kind distinguishes push (commit) events from tag events.
type Kind string

const (
	KindPush Kind = "push"
	KindTag  Kind = "tag"
)

// Event is a Git trigger event observed in a watched local repository.
type Event struct {
	ID        string    `json:"id"` // deterministic dedup key
	Kind      Kind      `json:"kind"`
	Repo      string    `json:"repo"`   // absolute path of the local git repo
	Commit    string    `json:"commit"` // full commit SHA
	Branch    string    `json:"branch,omitempty"`
	Tag       string    `json:"tag,omitempty"`
	Author    string    `json:"author,omitempty"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	SeenAt    time.Time `json:"seen_at,omitempty"`
}

// DedupKey returns the stable key used to suppress duplicate events.
// The key is kind+ref+commit so the same branch head delivered twice is a
// duplicate, while the same commit tagged twice with different tags is not.
func (e Event) DedupKey() string { return e.ID }

// Config is the declarative pipeline configuration.
type Config struct {
	Name        string   `json:"name"`
	Repo        string   `json:"repo"`
	Triggers    Triggers `json:"triggers"`
	Concurrency int      `json:"concurrency"` // max simultaneous runs, 0 = unlimited
	RunTimeout  Duration `json:"run_timeout"` // default per-run timeout, 0 = no limit
	Stages      []Stage  `json:"stages"`
}

// Triggers declares which events start a run.
type Triggers struct {
	Branches []string `json:"branches"` // glob patterns; empty means branch events do not trigger
	Tags     []string `json:"tags"`     // glob patterns; empty means tag events do not trigger
}

// Stage is a phase of a pipeline. Stages carrying the same non-empty Group run
// in parallel; stages in different groups (or with an empty group) run serially
// in declaration order. Needs optionally declares explicit upstream stage names.
type Stage struct {
	Name     string        `json:"name"`
	Group    string        `json:"group,omitempty"`
	Needs    []string      `json:"needs,omitempty"`
	Approval *ApprovalRule `json:"approval,omitempty"`
	Timeout  Duration      `json:"timeout,omitempty"` // stage timeout; 0 falls back to run timeout
	Retry    int           `json:"retry"`             // max automatic retries per failed task
	Tasks    []Task        `json:"tasks"`
}

// ApprovalRule configures a manual approval gate before a stage.
type ApprovalRule struct {
	Required  bool     `json:"required"`
	Approvers []string `json:"approvers,omitempty"`
	Timeout   Duration `json:"timeout,omitempty"` // pending approval is canceled after this duration
}

// Task is a single executable command within a stage.
type Task struct {
	Name         string   `json:"name"`
	Workdir      string   `json:"workdir,omitempty"`
	Command      string   `json:"command"`
	Args         []string `json:"args,omitempty"`
	Shell        bool     `json:"shell,omitempty"` // run via "sh -c" when true
	Env          []string `json:"env,omitempty"`
	AllowFailure bool     `json:"allow_failure,omitempty"`
	// Artifacts are glob patterns (relative to the workdir) collected into
	// the run's data directory after the task succeeds.
	Artifacts []string `json:"artifacts,omitempty"`
}

// Duration mirrors time.Duration and accepts both numbers (nanoseconds) and
// human strings such as "30m", "1h", "90s" in JSON/YAML configuration.
type Duration time.Duration

// Std returns the duration as a standard library time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// MarshalJSON renders the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts a duration string ("30m") or a numeric nanosecond value.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case string:
		parsed, err := time.ParseDuration(x)
		if err != nil {
			return err
		}
		*d = Duration(parsed)
	case float64:
		*d = Duration(time.Duration(int64(x)))
	case nil:
		*d = 0
	default:
		return errors.New("duration must be a string or number")
	}
	return nil
}

// Run is one pipeline execution instance.
type Run struct {
	ID         string       `json:"id"`
	Pipeline   string       `json:"pipeline"`
	EventID    string       `json:"event_id"`
	ConfigJSON []byte       `json:"config_json"` // snapshot of the config used by this run
	Branch     string       `json:"branch,omitempty"`
	Tag        string       `json:"tag,omitempty"`
	Commit     string       `json:"commit"`
	Status     Status       `json:"status"`
	Reason     string       `json:"reason,omitempty"`
	CreatedAt  time.Time    `json:"created_at"`
	StartedAt  *time.Time   `json:"started_at,omitempty"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
	Deadline   *time.Time   `json:"deadline,omitempty"`
	UpdatedAt  time.Time    `json:"updated_at"`
	Stages     []StageState `json:"stages"`
	Generation int          `json:"generation"` // bumped on every scheduling/retry cycle
}

// StageState is the execution state of one stage within a run.
type StageState struct {
	Name       string         `json:"name"`
	Group      string         `json:"group,omitempty"`
	Status     Status         `json:"status"`
	Attempts   int            `json:"attempts"`
	StartedAt  *time.Time     `json:"started_at,omitempty"`
	FinishedAt *time.Time     `json:"finished_at,omitempty"`
	Reason     string         `json:"reason,omitempty"`
	Approval   *ApprovalState `json:"approval,omitempty"`
	Tasks      []TaskState    `json:"tasks"`
}

// TaskState is the execution state of one task.
type TaskState struct {
	Name       string     `json:"name"`
	Status     Status     `json:"status"`
	Attempt    int        `json:"attempt"`
	ExitCode   int        `json:"exit_code"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Artifacts  []string   `json:"artifacts,omitempty"` // collected paths under the artifact root
}

// ApprovalState records a manual approval decision for audit.
type ApprovalState struct {
	Required  bool       `json:"required"`
	Status    Status     `json:"status"` // pending / success / failed(rejected) / canceled
	Approver  string     `json:"approver,omitempty"`
	Comment   string     `json:"comment,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	DecidedAt *time.Time `json:"decided_at,omitempty"`
	Deadline  *time.Time `json:"deadline,omitempty"`
}

// EventRecord marks an event that already produced a run (dedup).
type EventRecord struct {
	EventID string    `json:"event_id"`
	Kind    Kind      `json:"kind"`
	Repo    string    `json:"repo"`
	Commit  string    `json:"commit"`
	Branch  string    `json:"branch,omitempty"`
	Tag     string    `json:"tag,omitempty"`
	RunID   string    `json:"run_id"`
	SeenAt  time.Time `json:"seen_at"`
}

// Terminal reports whether the run reached a final state.
func (r *Run) Terminal() bool { return r.Status.IsTerminal() }

// Stage finds the state of the named stage.
func (r *Run) Stage(name string) *StageState {
	for i := range r.Stages {
		if r.Stages[i].Name == name {
			return &r.Stages[i]
		}
	}
	return nil
}

// PendingApproval returns the first stage waiting on an approval, if any.
func (r *Run) PendingApproval() *StageState {
	for i := range r.Stages {
		if r.Stages[i].Approval != nil && r.Stages[i].Approval.Status == StatusPending {
			return &r.Stages[i]
		}
	}
	return nil
}
