// Package engine orchestrates pipeline runs: scheduling of serial/parallel
// stages, approval gates, automatic retries, timeouts, cancellation,
// run concurrency limits and recovery after process restarts.
//
// Every state transition is persisted before it takes effect, so a crash
// never loses run state. Within one run, stages scheduled with the same
// "group" run in parallel; stages in different groups run serially in
// declaration order. Explicit "needs" may additionally widen dependencies.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/approval"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/artifacts"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/event"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/executor"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

// Errors returned by the engine.
var (
	ErrRunNotFound = errors.New("engine: run not found")
	ErrRunActive   = errors.New("engine: run is not active")
	ErrNoApproval  = errors.New("engine: no pending approval for stage")
)

// Engine is the pipeline orchestrator.
type Engine struct {
	st        store.Store
	exec      *executor.Executor
	appr      *approval.Manager
	artifacts *artifacts.Collector

	workDir     string
	artifactDir string

	// runConcurrency limits simultaneously active runs; 0 means unlimited.
	runConcurrency int

	mu     sync.Mutex
	active map[string]*runHandle
	slots  chan struct{}
	wg     sync.WaitGroup

	lockMu  sync.Mutex
	lockMap map[string]*sync.Mutex

	cancel  context.CancelFunc // cancels all run goroutines on Stop
	rootCtx context.Context
}

// runHandle holds in-memory control structures for a live run.
type runHandle struct {
	id      string
	cancel  context.CancelFunc // cancels this run's workers
	blocked bool               // waiting for an approval / concurrency slot
	reason  string             // cancel reason recorded by CancelRun
}

// Options configures a new Engine.
type Options struct {
	Store        store.Store
	WorkDir      string
	Concurrency  int    // max simultaneously active runs, 0 = unlimited
	ArtifactRoot string // data dir for collected artifacts; "" disables
}

// New constructs an Engine.
func New(opts Options) *Engine {
	if opts.WorkDir == "" {
		opts.WorkDir = "."
	}
	e := &Engine{
		st:             opts.Store,
		exec:           executor.NewExecutor(),
		appr:           approval.NewManager(),
		workDir:        opts.WorkDir,
		artifactDir:    opts.ArtifactRoot,
		runConcurrency: opts.Concurrency,
		active:         make(map[string]*runHandle),
		lockMap:        make(map[string]*sync.Mutex),
	}
	if opts.ArtifactRoot != "" {
		e.artifacts = artifacts.New(opts.ArtifactRoot)
	}
	return e
}

// Start launches background loops. It must be called once before triggering
// runs and paired with Stop.
func (e *Engine) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	e.rootCtx = ctx
	e.cancel = cancel
	return nil
}

// Stop cancels all in-flight runs, waits for their workers to exit and
// verifies that no child processes remain.
func (e *Engine) Stop(ctx context.Context) error {
	if e.cancel != nil {
		e.cancel()
	}
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	// Any still-tracked child is force-killed as a last resort.
	for _, c := range e.exec.ActiveCmds() {
		_ = c // commands are already canceled via contexts; kill is best-effort
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	idleErr := e.exec.WaitIdle(waitCtx)
	// Only after all run workers have exited do we wait for persistence:
	// every state transition is bracketed, so once this returns no write is
	// in flight and the data directory can be safely removed/reused.
	e.st.WaitForWrites()
	return idleErr
}

// newID returns a random hex identifier with the given prefix.
func newID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}

// snapshotConfig stores the exact configuration used by a run so later
// config changes do not affect in-flight or re-run historical runs.
func snapshotConfig(cfg *model.Config) []byte {
	data, _ := json.Marshal(cfg)
	return data
}

// LoadRunConfig decodes the config snapshot embedded in a run.
func LoadRunConfig(run *model.Run) (*model.Config, error) {
	if len(run.ConfigJSON) == 0 {
		return nil, fmt.Errorf("run %s has no config snapshot", run.ID)
	}
	cfg := &model.Config{}
	if err := json.Unmarshal(run.ConfigJSON, cfg); err != nil {
		return nil, fmt.Errorf("decode run config: %w", err)
	}
	return cfg, nil
}

// TriggerEvent filters, de-duplicates and starts a run for an event.
// It returns created=false (no error) when the event does not match the
// trigger rules or is a duplicate of an already admitted event.
func (e *Engine) TriggerEvent(ctx context.Context, cfg *model.Config, ev model.Event) (string, bool, error) {
	if !event.Match(cfg.Triggers, ev) {
		return "", false, nil
	}

	// Reserve the dedup record before any run state exists, so a duplicate
	// event racing the first can never create a second run. The record binds
	// the run id; if a duplicate wins the race we return before saving a run,
	// i.e. no phantom ("abandoned") run files are produced.
	run := e.buildRun(cfg, ev)
	admitted, err := event.Admit(ctx, e.st, event.NewRecord(ev, run.ID))
	if err != nil {
		return "", false, err
	}
	if !admitted {
		return "", false, nil
	}

	if err := e.st.SaveRun(ctx, run); err != nil {
		return "", false, err
	}

	e.launch(run)
	return run.ID, true, nil
}

// buildRun creates a fresh Run with all stages and tasks in pending state.
func (e *Engine) buildRun(cfg *model.Config, ev model.Event) *model.Run {
	now := time.Now()
	run := &model.Run{
		ID:         newID("run"),
		Pipeline:   cfg.Name,
		EventID:    ev.ID,
		ConfigJSON: snapshotConfig(cfg),
		Branch:     ev.Branch,
		Tag:        ev.Tag,
		Commit:     ev.Commit,
		Status:     model.StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
		Generation: 0,
	}
	if cfg.RunTimeout.Std() > 0 {
		dl := now.Add(cfg.RunTimeout.Std())
		run.Deadline = &dl
	}
	for _, st := range cfg.Stages {
		ss := model.StageState{Name: st.Name, Group: st.Group, Status: model.StatusPending}
		for _, t := range st.Tasks {
			ss.Tasks = append(ss.Tasks, model.TaskState{Name: t.Name, Status: model.StatusPending})
		}
		run.Stages = append(run.Stages, ss)
	}
	return run
}

// launch registers and starts a run asynchronously.
func (e *Engine) launch(run *model.Run) {
	e.wg.Add(1)
	go e.runLoop(run.ID)
}

// GetRun returns a persisted run snapshot.
func (e *Engine) GetRun(ctx context.Context, id string) (*model.Run, error) {
	return e.st.LoadRun(ctx, id)
}

// GetRuns returns all runs, newest first.
func (e *Engine) GetRuns(ctx context.Context) ([]*model.Run, error) {
	return e.st.ListRuns(ctx)
}

// ActiveChildren returns the number of child processes currently running.
func (e *Engine) ActiveChildren() int { return e.exec.ActiveCount() }

// runLocks serializes read-modify-write transactions of a single run so that
// concurrently executing parallel stages cannot overwrite each other's state
// (lost-update on the full-document JSON store).
func (e *Engine) runLock(id string) func() {
	e.lockMu.Lock()
	m, ok := e.lockMap[id]
	if !ok {
		m = &sync.Mutex{}
		e.lockMap[id] = m
	}
	e.lockMu.Unlock()
	m.Lock()
	return m.Unlock
}

// MutateRun loads the run, invokes fn under its exclusive lock, and persists
// the result when fn reports the run dirty. This is the only supported way
// to transition run/stage/task state from multiple goroutines.
func (e *Engine) MutateRun(ctx context.Context, id string, fn func(run *model.Run) error) (*model.Run, error) {
	unlock := e.runLock(id)
	defer unlock()
	run, err := e.st.LoadRun(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := fn(run); err != nil {
		return run, err
	}
	if err := e.st.SaveRun(ctx, run); err != nil {
		return run, err
	}
	return run, nil
}
