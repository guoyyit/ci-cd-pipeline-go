package engine

import (
	"context"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// RecoveryPolicy decides how unfinished runs found after a restart are handled.
type RecoveryPolicy string

const (
	// RecoveryResume resumes unfinished runs: completed stages are kept,
	// interrupted (running) stages are re-executed together with pending
	// downstream stages. No successful stage runs twice.
	RecoveryResume RecoveryPolicy = "resume"
	// RecoveryCancel marks every unfinished run canceled.
	RecoveryCancel RecoveryPolicy = "cancel"
	// RecoveryFail marks every unfinished run failed.
	RecoveryFail RecoveryPolicy = "fail"
)

// Recover scans persisted runs and applies the policy to runs that were not
// in a terminal state when the process exited. It must be called after Start
// and before new events are ingested.
//
// On disk a "running" status can only mean the process crashed mid-stage
// (there is no other way to leave it behind), so under RecoveryResume those
// stages - and any pending downstream stages - are reset to pending;
// successful stages are preserved, giving exactly-once effective execution.
func (e *Engine) Recover(ctx context.Context, policy RecoveryPolicy) ([]string, error) {
	runs, err := e.st.ListRuns(ctx)
	if err != nil {
		return nil, err
	}
	var recovered []string
	for _, run := range runs {
		if run.Status.IsTerminal() {
			continue
		}
		recovered = append(recovered, run.ID)
		switch policy {
		case RecoveryCancel, RecoveryFail:
			status := model.StatusCanceled
			reason := "canceled after daemon restart"
			if policy == RecoveryFail {
				status = model.StatusFailed
				reason = "marked failed after daemon restart"
			}
			e.markUnfinished(ctx, run, status, status, reason)
		case RecoveryResume:
			e.resumeRun(ctx, run)
		default:
			e.markUnfinished(ctx, run, model.StatusCanceled, model.StatusCanceled, "unknown recovery policy")
		}
	}
	return recovered, nil
}

// resumeRun prepares and relaunches one unfinished run after a crash.
func (e *Engine) resumeRun(ctx context.Context, run *model.Run) {
	cfg, err := LoadRunConfig(run)
	if err != nil {
		e.finish(ctx, run, model.StatusFailed, "cannot resume: config snapshot unreadable")
		return
	}
	deps := buildDependencies(cfg)
	tainted := map[string]bool{}

	now := time.Now()
	for i := range run.Stages {
		st := &run.Stages[i]
		switch st.Status {
		case model.StatusRunning:
			// Interrupted by crash: must re-execute; taint downstream.
			tainted[st.Name] = true
			resetStageState(st, "interrupted by daemon restart")
		case model.StatusPending:
			// Keep pending; it may become tainted by an interrupted upstream.
		case model.StatusWaiting:
			// Was blocked on upstream stages or an approval gate. The wait
			// semantics survive the restart: the stage is not pending and
			// the scheduler blocks/launches it from exactly this state.
		}
	}
	// Taint every stage that (transitively) depends on an interrupted stage.
	changed := true
	for changed {
		changed = false
		for _, st := range cfg.Stages {
			if tainted[st.Name] {
				continue
			}
			for _, d := range deps[st.Name] {
				if tainted[d] {
					tainted[st.Name] = true
					changed = true
					break
				}
			}
		}
	}
	for i := range run.Stages {
		st := &run.Stages[i]
		if tainted[st.Name] {
			// Interrupted stages and any dependent stage (even a previously
			// successful one) are reset: a success downstream of an
			// interrupted stage cannot be trusted.
			resetStageState(st, "downstream of interrupted stage")
		}
		// Re-arm a pending approval gate so Approve/Reject work again.
		if st.Approval != nil && st.Approval.Status == model.StatusPending {
			e.appr.Adopt(ctx, run.ID, st.Name, st.Approval)
		}
	}

	// Restore the waiting semantics in the same write: stages that were
	// blocked on unfinished upstreams at shutdown are waiting, not pending.
	// (A pending stage whose upstreams are all done will simply be launched
	// by the first scheduler tick.)
	for i := range run.Stages {
		st := &run.Stages[i]
		if st.Status != model.StatusPending || tainted[st.Name] {
			continue
		}
		for _, dep := range deps[st.Name] {
			ds := run.Stage(dep)
			if ds != nil && !ds.Status.IsTerminal() {
				st.Status = model.StatusWaiting
				break
			}
		}
	}

	// Run deadlines that already passed are honored immediately on resume.
	if run.Deadline != nil && !run.Deadline.After(now) {
		e.markUnfinished(ctx, run, model.StatusCanceled, model.StatusCanceled, "run deadline exceeded before restart")
		return
	}

	// Preserve the waiting semantics on the run itself: if any stage is
	// still waiting, the run resumes as waiting; otherwise it re-enters the
	// queue as pending (the scheduler promotes it on the first tick).
	run.Status = deriveRunStatus(run)
	run.StartedAt = nil
	run.FinishedAt = nil
	run.Reason = "resumed after daemon restart"
	run.UpdatedAt = now
	run.Generation++
	if err := e.st.SaveRun(ctx, run); err != nil {
		return
	}
	e.launch(run)
}

// resetStageState returns a stage (and its tasks) to pending for re-execution.
func resetStageState(st *model.StageState, reason string) {
	_ = reason
	st.Status = model.StatusPending
	st.Attempts = 0
	st.StartedAt = nil
	st.FinishedAt = nil
	st.Reason = ""
	st.Approval = nil
	for j := range st.Tasks {
		name := st.Tasks[j].Name
		st.Tasks[j] = model.TaskState{Name: name, Status: model.StatusPending}
	}
}
