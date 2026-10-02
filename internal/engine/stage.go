package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/executor"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// executeStage runs one stage to completion: optional approval gate, then
// each task serially with up to Retry automatic re-executions. All run-state
// updates go through MutateRun so parallel stages never clobber one another.
func (s *scheduler) executeStage(ctx context.Context, runID string, stageIdx int) {
	stCfg := s.stageCfgByIndex(stageIdx)
	if stCfg == nil {
		_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
			s.setFail(run, stageIdx, "stage config missing from snapshot")
			return nil
		})
		return
	}

	// Stage-scoped timeout.
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var timerCh <-chan time.Time
	if stCfg.Timeout.Std() > 0 {
		t := time.NewTimer(stCfg.Timeout.Std())
		defer t.Stop()
		timerCh = t.C
	}
	var timedOut atomic.Bool
	go func() {
		select {
		case <-timerCh:
			timedOut.Store(true)
			cancel()
		case <-sctx.Done():
		}
	}()

	// --- Approval gate -----------------------------------------------------
	if stCfg.Approval != nil && stCfg.Approval.Required {
		if sctx.Err() != nil {
			// Canceled before the gate was reached: do not open a spurious
			// approval request on an already-canceled run.
			_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
				s.setCancel(run, stageIdx, s.reason(timedOut.Load()))
				return nil
			})
			return
		}
		decision, err := s.awaitApproval(sctx, runID, stCfg)
		if err != nil && decision == nil {
			_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
				s.setFail(run, stageIdx, "approval error: "+err.Error())
				return nil
			})
			return
		}
		if decision != nil && decision.Status != model.StatusSuccess {
			reason := "approval: " + decision.Comment
			_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
				if decision.Status == model.StatusCanceled {
					s.setCancel(run, stageIdx, reason)
				} else {
					s.setFail(run, stageIdx, reason)
				}
				return nil
			})
			return
		}
		// If the run was canceled while waiting, nothing more to do.
		run, _ := s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error { return nil })
		if run != nil && run.Status.IsTerminal() {
			return
		}
	}

	// Mark running.
	if _, err := s.eng.MutateRun(sctx, runID, func(run *model.Run) error {
		if run.Status.IsTerminal() {
			return errRunTerminal
		}
		now := time.Now()
		st := &run.Stages[stageIdx]
		st.Status = model.StatusRunning
		st.StartedAt = &now
		run.UpdatedAt = now
		return nil
	}); err != nil {
		return
	}

	// --- Tasks -------------------------------------------------------------
	maxAttempts := stCfg.Retry + 1
	for _, task := range stCfg.Tasks {
		if sctx.Err() != nil {
			_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
				s.setCancel(run, stageIdx, s.reason(timedOut.Load()))
				return nil
			})
			return
		}
		var taskErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if sctx.Err() != nil {
				break
			}
			taskErr = s.runOneTask(sctx, runID, stageIdx, task, attempt)
			if taskErr == nil || errors.Is(taskErr, executor.ErrCanceled) {
				break
			}
		}
		if taskErr != nil {
			if errors.Is(taskErr, executor.ErrCanceled) || sctx.Err() != nil {
				_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
					s.setCancel(run, stageIdx, s.reason(timedOut.Load()))
					return nil
				})
				return
			}
			if !task.AllowFailure {
				_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
					s.setFail(run, stageIdx, "task "+task.Name+" failed")
					return nil
				})
				return
			}
		}
	}

	_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
		st := &run.Stages[stageIdx]
		now := time.Now()
		for j := range st.Tasks {
			if !st.Tasks[j].Status.IsTerminal() {
				st.Tasks[j].Status = model.StatusSkipped
				st.Tasks[j].FinishedAt = &now
			}
		}
		st.Status = model.StatusSuccess
		st.Reason = ""
		st.FinishedAt = &now
		run.UpdatedAt = now
		return nil
	})
}

var errRunTerminal = errors.New("run already terminal")

func (s *scheduler) reason(timedOut bool) string {
	if timedOut {
		return "stage timeout exceeded"
	}
	return "stage canceled"
}

// stageCfgByIndex returns the config of the stage at declaration index idx.
func (s *scheduler) stageCfgByIndex(idx int) *model.Stage {
	if s.cfg == nil || idx < 0 || idx >= len(s.cfg.Stages) {
		return nil
	}
	return &s.cfg.Stages[idx]
}

// awaitApproval registers and persists a pending approval and blocks for the
// decision. It also persists the final manager-side decision (timeout).
func (s *scheduler) awaitApproval(ctx context.Context, runID string, stCfg *model.Stage) (*model.ApprovalState, error) {
	state, err := s.eng.appr.Request(ctx, runID, stCfg.Name, stCfg.Approval)
	if err != nil {
		return nil, err
	}
	if _, err := s.eng.MutateRun(ctx, runID, func(run *model.Run) error {
		if st := run.Stage(stCfg.Name); st != nil {
			st.Approval = state
			// The stage is now parked at the approval gate: surface the
			// waiting state instead of leaving it pending/running.
			if st.Status == model.StatusPending || st.Status == model.StatusWaiting {
				st.Status = model.StatusWaiting
			}
			run.UpdatedAt = time.Now()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	decision, waitErr := s.eng.appr.Wait(ctx, runID, stCfg.Name)
	_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
		if st := run.Stage(stCfg.Name); st != nil && decision != nil {
			st.Approval = decision
			run.UpdatedAt = time.Now()
		}
		return nil
	})
	return decision, waitErr
}

func (s *scheduler) runOneTask(ctx context.Context, runID string, stageIdx int, task model.Task, attempt int) error {
	if _, err := s.eng.MutateRun(ctx, runID, func(run *model.Run) error {
		stage := &run.Stages[stageIdx]
		tIdx := -1
		for i := range stage.Tasks {
			if stage.Tasks[i].Name == task.Name {
				tIdx = i
				break
			}
		}
		if tIdx < 0 {
			return errors.New("task state not found: " + task.Name)
		}
		now := time.Now()
		stage.Attempts = attempt
		stage.Tasks[tIdx].Status = model.StatusRunning
		stage.Tasks[tIdx].Attempt = attempt
		stage.Tasks[tIdx].StartedAt = &now
		stage.Tasks[tIdx].FinishedAt = nil
		stage.Tasks[tIdx].ExitCode = 0
		run.UpdatedAt = now
		return nil
	}); err != nil {
		return err
	}

	res, runErr := s.eng.exec.RunTask(ctx, task, s.eng.workDir)

	// Collect artifacts only after a successful, non-canceled task.
	var collected []string
	if runErr == nil && s.eng.artifacts != nil {
		taskWorkdir := s.eng.workDir
		if task.Workdir != "" {
			taskWorkdir = resolveTaskWorkdir(s.eng.workDir, task.Workdir)
		}
		files, cerr := s.eng.artifacts.Collect(runID, s.stageName(stageIdx), task.Name, taskWorkdir, task.Artifacts)
		if cerr == nil {
			collected = files
		}
	}

	_, _ = s.eng.MutateRun(context.Background(), runID, func(run *model.Run) error {
		stage := &run.Stages[stageIdx]
		for i := range stage.Tasks {
			if stage.Tasks[i].Name == task.Name {
				fin := time.Now()
				stage.Tasks[i].FinishedAt = &fin
				stage.Tasks[i].ExitCode = res.ExitCode
				if len(collected) > 0 {
					stage.Tasks[i].Artifacts = collected
				}
				switch {
				case runErr == nil:
					stage.Tasks[i].Status = model.StatusSuccess
				case errors.Is(runErr, executor.ErrCanceled) || ctx.Err() != nil:
					stage.Tasks[i].Status = model.StatusCanceled
				default:
					stage.Tasks[i].Status = model.StatusFailed
				}
				run.UpdatedAt = fin
				break
			}
		}
		return nil
	})
	return runErr
}

// stageName returns the declared name of the stage at declaration index idx.
func (s *scheduler) stageName(idx int) string {
	if st := s.stageCfgByIndex(idx); st != nil {
		return st.Name
	}
	return ""
}

func resolveTaskWorkdir(base, sub string) string {
	if filepath.IsAbs(sub) {
		return sub
	}
	return filepath.Join(base, sub)
}

// setFail transitions one stage to failed, skipping unfinished tasks.
func (s *scheduler) setFail(run *model.Run, stageIdx int, reason string) {
	now := time.Now()
	st := &run.Stages[stageIdx]
	st.Status = model.StatusFailed
	st.Reason = reason
	st.FinishedAt = &now
	for j := range st.Tasks {
		if !st.Tasks[j].Status.IsTerminal() {
			st.Tasks[j].Status = model.StatusSkipped
			st.Tasks[j].FinishedAt = &now
		}
	}
	if st.Approval != nil && st.Approval.Status == model.StatusPending {
		st.Approval.Status = model.StatusFailed
		st.Approval.Comment = reason
		st.Approval.DecidedAt = &now
	}
	s.eng.appr.Remove(run.ID, st.Name)
	run.UpdatedAt = now
}

// setCancel transitions one stage to canceled.
func (s *scheduler) setCancel(run *model.Run, stageIdx int, reason string) {
	now := time.Now()
	st := &run.Stages[stageIdx]
	st.Status = model.StatusCanceled
	st.Reason = reason
	st.FinishedAt = &now
	for j := range st.Tasks {
		if !st.Tasks[j].Status.IsTerminal() {
			st.Tasks[j].Status = model.StatusCanceled
			st.Tasks[j].FinishedAt = &now
		}
	}
	if st.Approval != nil && st.Approval.Status == model.StatusPending {
		st.Approval.Status = model.StatusCanceled
		st.Approval.Comment = reason
		st.Approval.DecidedAt = &now
	}
	s.eng.appr.Remove(run.ID, st.Name)
	run.UpdatedAt = now
}
