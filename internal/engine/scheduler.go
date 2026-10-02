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

	// Plan the pass against the loaded snapshot. Every transition decided
	// here is committed in one atomic document update together with the
	// derived run status, so observers never see a waiting stage under a
	// pending/running run and a concurrent cancel resolves against a single
	// consistent document.
	progress := false
	waiting := false
	var toLaunch []int

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

		switch decide(s.dependencyStates(run, st.Name)) {
		case depBlocked:
			if st.Status != model.StatusWaiting {
				progress = true
			}
			st.Status = model.StatusWaiting
			waiting = true
		case depSkip:
			s.skipStageLocked(run, i, "upstream stage did not succeed")
			progress = true
		case depReady:
			s.mu.Lock()
			if !s.launched[st.Name] {
				s.launched[st.Name] = true
				toLaunch = append(toLaunch, i)
				// The stage is handed to a worker this pass; until that
				// worker records running (or approval-wait), surface it as
				// waiting rather than a never-dispatched pending stage.
				if st.Status == model.StatusPending {
					st.Status = model.StatusWaiting
				}
			}
			s.mu.Unlock()
			waiting = true
			progress = true
		}
	}

	committed, _ := s.eng.MutateRun(ctx, run.ID, func(r *model.Run) error {
		if r.Status.IsTerminal() {
			return errRunTerminal
		}
		// Copy the planned stage statuses into the freshly locked document.
		for i := range r.Stages {
			planned := run.Stages[i]
			cur := &r.Stages[i]
			if cur.Status.IsTerminal() || cur.Status == planned.Status {
				continue
			}
			if planned.Status.IsTerminal() {
				cur.Status = planned.Status
				cur.Reason = planned.Reason
				cur.FinishedAt = planned.FinishedAt
				for j := range cur.Tasks {
					if j < len(planned.Tasks) && planned.Tasks[j].Status.IsTerminal() {
						cur.Tasks[j].Status = planned.Tasks[j].Status
						cur.Tasks[j].FinishedAt = planned.FinishedAt
					}
				}
				continue
			}
			cur.Status = planned.Status
		}
		if s.allTerminal(r) {
			s.finalizeRunLocked(r)
			return nil
		}
		r.Status = deriveRunStatus(r)
		r.UpdatedAt = time.Now()
		return nil
	})
	if committed == nil || committed.Status.IsTerminal() {
		// A concurrent cancel committed first: start nothing.
		return actionDone, nil
	}

	// Workers start only after the commit; each one re-checks the persisted
	// terminal state before running anything.
	for _, idx := range toLaunch {
		stageIdx := idx
		s.eng.wg.Add(1)
		go func() {
			defer s.eng.wg.Done()
			s.executeStage(ctx, run.ID, stageIdx)
		}()
	}

	if s.allTerminal(committed) {
		return actionDone, nil
	}
	if progress {
		return actionProgress, nil
	}
	if waiting {
		return actionWaiting, nil
	}
	return actionDone, fmt.Errorf("scheduler stuck for run %s", run.ID)
}

type depDecision int

const (
	depBlocked depDecision = iota // dependencies still pending/running
	depSkip                       // a dependency failed/canceled/skipped
	depReady                      // all dependencies succeeded
)

func decide(deps []model.Status) depDecision {
	if len(deps) == 0 {
		return depReady
	}
	allSuccess := true
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

// deriveRunStatus computes the run status from non-terminal stages:
// any executing stage means running; otherwise any blocked-but-dispatched
// stage means waiting; otherwise the run is still queued (pending).
func deriveRunStatus(run *model.Run) model.Status {
	hasWaiting, hasPending := false, false
	for i := range run.Stages {
		switch run.Stages[i].Status {
		case model.StatusRunning:
			return model.StatusRunning
		case model.StatusWaiting:
			hasWaiting = true
		case model.StatusPending:
			hasPending = true
		}
	}
	switch {
	case hasWaiting:
		return model.StatusWaiting
	case hasPending:
		return model.StatusPending
	default:
		return model.StatusRunning
	}
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
