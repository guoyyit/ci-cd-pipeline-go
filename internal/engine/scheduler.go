package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// tickAction tells the run loop what happened during a scheduler tick.
type tickAction int

const (
	actionDone     tickAction = iota // run reached a terminal state
	actionWaiting                    // blocked on an approval
	actionProgress                   // a stage was launched or skipped
)

// scheduler computes the stage DAG and launches stages when their
// dependencies are satisfied. Stage workers persist their own progress.
type scheduler struct {
	eng  *Engine
	run  *model.Run
	cfg  *model.Config
	deps map[string][]string // stage name -> dependency stage names

	mu       sync.Mutex
	launched map[string]bool // stages whose worker goroutine has been started
}

func newScheduler(eng *Engine, run *model.Run) *scheduler {
	cfg, err := LoadRunConfig(run)
	if err != nil {
		// Without a config snapshot the run cannot execute; caller treats
		// nil cfg as a fatal scheduler error.
		cfg = nil
	}
	s := &scheduler{
		eng:      eng,
		run:      run,
		cfg:      cfg,
		deps:     buildDependencies(cfg),
		launched: make(map[string]bool),
	}
	return s
}

// effectiveGroup returns the stage's parallel group; unnamed stages get a
// unique per-index group so they run serially.
func effectiveGroup(st model.Stage, idx int) string {
	if st.Group != "" {
		return st.Group
	}
	return fmt.Sprintf("__serial_%d", idx)
}

// buildDependencies computes each stage's upstream dependency list from
// explicit Needs (authoritative when present) or group ordering: a stage
// depends on every stage in groups that appear earlier in declaration order.
func buildDependencies(cfg *model.Config) map[string][]string {
	deps := make(map[string][]string)
	if cfg == nil {
		return deps
	}
	groupOrder := map[string]int{}
	groups := make([]string, 0, len(cfg.Stages))
	effGroup := make([]string, len(cfg.Stages))
	for i, st := range cfg.Stages {
		g := effectiveGroup(st, i)
		effGroup[i] = g
		if _, ok := groupOrder[g]; !ok {
			groupOrder[g] = len(groups)
			groups = append(groups, g)
		}
	}
	for i, st := range cfg.Stages {
		if len(st.Needs) > 0 {
			deps[st.Name] = append([]string{}, st.Needs...)
			continue
		}
		gi := groupOrder[effGroup[i]]
		for j, prev := range cfg.Stages {
			if j == i {
				continue
			}
			if groupOrder[effGroup[j]] < gi {
				deps[st.Name] = append(deps[st.Name], prev.Name)
			}
		}
	}
	return deps
}

func (s *scheduler) stageCfg(name string) *model.Stage {
	for i := range s.cfg.Stages {
		if s.cfg.Stages[i].Name == name {
			return &s.cfg.Stages[i]
		}
	}
	return nil
}

// tick evaluates one scheduling pass against freshly persisted state.
func (s *scheduler) tick(ctx context.Context) (tickAction, error) {
	if s.cfg == nil {
		return actionDone, fmt.Errorf("missing config snapshot for run %s", s.run.ID)
	}
	run, err := s.eng.st.LoadRun(ctx, s.run.ID)
	if err != nil {
		return actionDone, err
	}
	s.run = run

	progress := false
	waiting := false

	for i := range run.Stages {
		st := &run.Stages[i]
		if st.Status.IsTerminal() {
			continue
		}

		s.mu.Lock()
		already := s.launched[st.Name]
		s.mu.Unlock()
		if already {
			// Worker goroutine owns the stage (executing or blocked at gate).
			waiting = true
			continue
		}

		depStates := s.dependencyStates(run, st.Name)
		switch decide(depStates) {
		case depBlocked:
			waiting = true
			if st.Status != model.StatusWaiting {
				idx := i
				_, _ = s.eng.MutateRun(ctx, run.ID, func(r *model.Run) error {
					s.markWaitingLocked(r, idx)
					return nil
				})
				// Mirror into this tick's copy so the run-status derivation
				// below observes the transition immediately.
				st.Status = model.StatusWaiting
			}
			continue
		case depSkip:
			idx := i
			_, _ = s.eng.MutateRun(ctx, run.ID, func(r *model.Run) error {
				s.skipStageLocked(r, idx, "upstream stage did not succeed")
				return nil
			})
			progress = true
			continue
		case depReady:
			if ctx.Err() != nil {
				// The run was canceled concurrently; never launch a new
				// stage after cancellation.
				waiting = true
				continue
			}
			idx := i
			s.mu.Lock()
			s.launched[st.Name] = true
			s.mu.Unlock()
			s.eng.wg.Add(1)
			go func() {
				defer s.eng.wg.Done()
				s.executeStage(ctx, run.ID, idx)
			}()
			progress = true
		}
	}

	if s.allTerminal(run) {
		_, _ = s.eng.MutateRun(ctx, run.ID, func(r *model.Run) error {
			s.finalizeRunLocked(r)
			return nil
		})
		return actionDone, nil
	}
	s.syncRunStatus(ctx, run)
	if progress {
		return actionProgress, nil
	}
	if waiting {
		return actionWaiting, nil
	}
	// Nothing running, nothing waiting, but not all terminal: this should be
	// unreachable; treat it as a failure rather than spinning forever.
	return actionDone, fmt.Errorf("scheduler stuck for run %s", run.ID)
}

type depDecision int

const (
	depBlocked depDecision = iota // dependencies still pending/running
	depSkip                       // a dependency failed/canceled/skipped
	depReady                      // all dependencies succeeded
)

func decide(deps []model.Status) depDecision {
	allSuccess := len(deps) > 0
	if len(deps) == 0 {
		return depReady
	}
	for _, d := range deps {
		switch d {
		case model.StatusSuccess:
		case model.StatusPending, model.StatusWaiting, model.StatusRunning:
			allSuccess = false
		default: // failed / canceled / skipped
			return depSkip
		}
	}
	if allSuccess {
		return depReady
	}
	return depBlocked
}

func (s *scheduler) dependencyStates(run *model.Run, stageName string) []model.Status {
	var out []model.Status
	for _, dep := range s.deps[stageName] {
		if ds := run.Stage(dep); ds != nil {
			out = append(out, ds.Status)
		}
	}
	return out
}

// skipStageLocked marks a stage skipped; caller must hold the run lock.
func (s *scheduler) skipStageLocked(run *model.Run, idx int, reason string) {
	now := time.Now()
	st := &run.Stages[idx]
	st.Status = model.StatusSkipped
	st.Reason = reason
	st.FinishedAt = &now
	for j := range st.Tasks {
		st.Tasks[j].Status = model.StatusSkipped
		st.Tasks[j].FinishedAt = &now
	}
	run.UpdatedAt = now
}

// markWaitingLocked transitions a dependency-blocked stage to waiting.
// Caller must hold the run lock.
func (s *scheduler) markWaitingLocked(run *model.Run, idx int) {
	st := &run.Stages[idx]
	if st.Status != model.StatusPending {
		return
	}
	st.Status = model.StatusWaiting
	run.UpdatedAt = time.Now()
}

// isLaunched reports whether a worker goroutine owns the stage.
func (s *scheduler) isLaunched(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.launched[name]
}

// deriveRunStatus computes the run's display status from its stage states:
// any executing stage means running; otherwise any stage blocked on upstream
// stages or parked at an approval gate means waiting. It returns "" when the
// current status should be kept. A launched stage that has not persisted a
// pending approval yet is still starting up, so it counts as running.
func (s *scheduler) deriveRunStatus(run *model.Run) model.Status {
	waiting := false
	for i := range run.Stages {
		st := &run.Stages[i]
		switch st.Status {
		case model.StatusRunning:
			return model.StatusRunning
		case model.StatusWaiting:
			if st.Approval != nil && st.Approval.Status == model.StatusPending {
				waiting = true
				continue
			}
			if s.isLaunched(st.Name) {
				return model.StatusRunning
			}
			waiting = true
		}
	}
	if waiting {
		return model.StatusWaiting
	}
	return ""
}

// syncRunStatus persists a running<->waiting transition when the derived
// status differs from the persisted one. Queued (pending) and terminal runs
// are never touched, so a concurrent cancel always wins.
func (s *scheduler) syncRunStatus(ctx context.Context, run *model.Run) {
	cur := run.Status
	if cur != model.StatusRunning && cur != model.StatusWaiting {
		return
	}
	if want := s.deriveRunStatus(run); want == "" || want == cur {
		return
	}
	_, _ = s.eng.MutateRun(ctx, run.ID, func(r *model.Run) error {
		if r.Status != model.StatusRunning && r.Status != model.StatusWaiting {
			return nil
		}
		if w := s.deriveRunStatus(r); w != "" && w != r.Status {
			r.Status = w
			r.UpdatedAt = time.Now()
		}
		return nil
	})
}

func (s *scheduler) allTerminal(run *model.Run) bool {
	for i := range run.Stages {
		if !run.Stages[i].Status.IsTerminal() {
			return false
		}
	}
	return true
}

// finalizeRunLocked derives the run result from final stage states.
// Caller must hold the run lock.
func (s *scheduler) finalizeRunLocked(run *model.Run) {
	var failed, canceled bool
	for i := range run.Stages {
		switch run.Stages[i].Status {
		case model.StatusFailed, model.StatusSkipped:
			failed = true
		case model.StatusCanceled:
			canceled = true
		}
	}
	now := time.Now()
	switch {
	case failed:
		run.Status = model.StatusFailed
		if run.Reason == "" {
			run.Reason = "one or more stages failed"
		}
	case canceled:
		run.Status = model.StatusCanceled
		if run.Reason == "" {
			run.Reason = "one or more stages canceled"
		}
	default:
		run.Status = model.StatusSuccess
	}
	run.FinishedAt = &now
	run.UpdatedAt = now
	for i := range run.Stages {
		s.eng.appr.Remove(run.ID, run.Stages[i].Name)
	}
}
