package engine

import (
	"context"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/approval"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// CancelRun cancels a running or waiting run. Cancellation propagates to the
// currently executing child process (whole process group is killed) and
// pending downstream stages are marked canceled. Already-finished stages keep
// their result.
func (e *Engine) CancelRun(ctx context.Context, id, reason string) error {
	why := reasonOr(reason, "canceled by user")

	// Persist the terminal decision under the run lock before signaling the
	// context. Any state transition that commits after this sees a terminal
	// run, so no new stage/task can start once the cancel is accepted.
	unlock := e.runLock(id)
	run, err := e.st.LoadRun(ctx, id)
	if err != nil {
		unlock()
		return err
	}
	if run.Status.IsTerminal() {
		unlock()
		return ErrRunActive
	}
	now := time.Now()
	for i := range run.Stages {
		st := &run.Stages[i]
		if st.Status.IsTerminal() {
			continue
		}
		st.Status = model.StatusCanceled
		st.Reason = why
		st.FinishedAt = &now
		for j := range st.Tasks {
			if !st.Tasks[j].Status.IsTerminal() {
				st.Tasks[j].Status = model.StatusCanceled
				st.Tasks[j].FinishedAt = &now
			}
		}
		if st.Approval != nil && st.Approval.Status == model.StatusPending {
			st.Approval.Status = model.StatusCanceled
			st.Approval.Comment = why
			st.Approval.DecidedAt = &now
		}
	}
	run.Status = model.StatusCanceled
	run.Reason = why
	run.FinishedAt = &now
	run.UpdatedAt = now
	if err := e.st.SaveRun(ctx, run); err != nil {
		unlock()
		return err
	}

	e.mu.Lock()
	h, active := e.active[id]
	var ready <-chan struct{}
	if active {
		ready = h.ready
	}
	e.mu.Unlock()
	unlock()

	// Wake the approval gate (if any) and interrupt running children.
	for i := range run.Stages {
		if run.Stages[i].Approval != nil {
			e.appr.Cancel(id, run.Stages[i].Name, why)
		}
	}
	if active {
		// If the run loop has not bound yet, wait for it so the signal is
		// never lost; it exits immediately because the run is terminal.
		if ready != nil {
			select {
			case <-ready:
			case <-ctx.Done():
			}
		}
		e.mu.Lock()
		h2, stillActive := e.active[id]
		e.mu.Unlock()
		if stillActive {
			h2.cancel()
		}
	}
	return nil
}

// Approve grants a pending stage approval and records the approver.
func (e *Engine) Approve(ctx context.Context, runID, stage, approver string) (*model.Run, error) {
	return e.decideApproval(ctx, runID, stage, approver, "", true)
}

// Reject denies a pending stage approval. A non-empty comment is mandatory
// and is stored on the run so the rejection is traceable in the persisted
// audit record.
func (e *Engine) Reject(ctx context.Context, runID, stage, approver, comment string) (*model.Run, error) {
	return e.decideApproval(ctx, runID, stage, approver, comment, false)
}

func (e *Engine) decideApproval(ctx context.Context, runID, stage, approver, comment string, grant bool) (*model.Run, error) {
	run, err := e.st.LoadRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	cfg, err := LoadRunConfig(run)
	if err != nil {
		return nil, err
	}
	var rule *model.ApprovalRule
	for _, st := range cfg.Stages {
		if st.Name == stage {
			rule = st.Approval
			break
		}
	}
	if rule == nil {
		return nil, ErrNoApproval
	}
	if approver == "" {
		return nil, approval.ErrApprover
	}
	if !approval.Allowed(rule, approver) {
		return nil, approval.ErrApprover
	}

	// The stage worker may not have registered the in-memory gate yet (the
	// run is pending and the worker is still reaching the approval step), or
	// the daemon restarted. Pre-register from the persisted pending state so
	// the decision is accepted and recorded; Wait() then observes it.
	if _, perr := e.appr.Peek(runID, stage); perr != nil {
		now := time.Now()
		pre := &model.ApprovalState{
			Required: true, Status: model.StatusPending,
			CreatedAt: now,
		}
		if rule.Timeout.Std() > 0 {
			dl := now.Add(rule.Timeout.Std())
			pre.Deadline = &dl
		}
		e.appr.Adopt(ctx, runID, stage, pre)
	}

	var state *model.ApprovalState
	if grant {
		state, err = e.appr.Approve(ctx, runID, stage, approver)
	} else {
		state, err = e.appr.Reject(ctx, runID, stage, approver, comment)
	}
	if err != nil {
		return nil, err
	}

	return e.MutateRun(ctx, runID, func(r *model.Run) error {
		if ss := r.Stage(stage); ss != nil {
			ss.Approval = state
			r.UpdatedAt = time.Now()
		}
		return nil
	})
}

// RerunRun manually re-runs a run. With force=false only stages that did not
// succeed are reset to pending and re-executed; successful stages are reused
// and never run twice. With force=true every stage is reset (full re-run).
// A run that is still active cannot be re-run.
func (e *Engine) RerunRun(ctx context.Context, id string, force bool) (*model.Run, error) {
	run, err := e.st.LoadRun(ctx, id)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	_, active := e.active[id]
	e.mu.Unlock()
	if active || !run.Status.IsTerminal() {
		return nil, ErrRunActive
	}
	cfg, err := LoadRunConfig(run)
	if err != nil {
		return nil, err
	}
	deps := buildDependencies(cfg)

	now := time.Now()
	reset := make(map[string]bool)
	for i := range run.Stages {
		if force || run.Stages[i].Status != model.StatusSuccess {
			reset[run.Stages[i].Name] = true
		}
	}
	// Downstream propagation: any stage that (transitively) depends on a
	// reset stage must itself be reset even if it had succeeded.
	if !force {
		changed := true
		for changed {
			changed = false
			for _, st := range cfg.Stages {
				if reset[st.Name] {
					continue
				}
				for _, d := range deps[st.Name] {
					if reset[d] {
						reset[st.Name] = true
						changed = true
						break
					}
				}
			}
		}
	}

	for i := range run.Stages {
		if !reset[run.Stages[i].Name] {
			continue
		}
		st := &run.Stages[i]
		st.Status = model.StatusPending
		st.Attempts = 0
		st.StartedAt = nil
		st.FinishedAt = nil
		st.Reason = ""
		st.Approval = nil
		for j := range st.Tasks {
			st.Tasks[j] = model.TaskState{Name: st.Tasks[j].Name, Status: model.StatusPending}
		}
	}

	run.Status = model.StatusPending
	run.Reason = ""
	run.StartedAt = nil
	run.FinishedAt = nil
	run.UpdatedAt = now
	run.Generation++
	if cfg.RunTimeout.Std() > 0 {
		dl := now.Add(cfg.RunTimeout.Std())
		run.Deadline = &dl
	}
	if err := e.st.SaveRun(ctx, run); err != nil {
		return nil, err
	}
	e.launch(run)
	return run, nil
}

func reasonOr(r, fallback string) string {
	if r == "" {
		return fallback
	}
	return r
}
