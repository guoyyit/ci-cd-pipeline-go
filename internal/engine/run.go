package engine

import (
	"context"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// runLoop is the goroutine driving one run: acquire a concurrency slot,
// schedule stages until the run reaches a terminal state, then finalize.
func (e *Engine) runLoop(runID string) {
	defer e.wg.Done()

	rootCtx := e.rootCtx
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(rootCtx)
	defer cancel()

	h := &runHandle{id: runID, cancel: cancel}
	e.mu.Lock()
	e.active[runID] = h
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.active, runID)
		e.mu.Unlock()
	}()

	run, err := e.st.LoadRun(ctx, runID)
	if err != nil {
		return
	}
	var timeoutCh <-chan time.Time
	if run.Deadline != nil {
		d := time.Until(*run.Deadline)
		if d <= 0 {
			e.finish(ctx, run, model.StatusCanceled, "run deadline already exceeded at start")
			return
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timeoutCh = t.C
	}

	if err := e.acquireSlot(ctx, h); err != nil {
		run, _ = e.st.LoadRun(context.Background(), runID)
		if run != nil && !run.Status.IsTerminal() {
			e.finish(context.Background(), run, model.StatusCanceled, e.cancelReason(h, "engine stopped before run started"))
		}
		return
	}
	defer e.releaseSlot()

	now := time.Now()
	run, err = e.st.LoadRun(ctx, runID)
	if err != nil {
		return
	}
	if run.Status.IsTerminal() {
		return
	}
	sched := newScheduler(e, run)
	// A resumed run whose stages are all still blocked (e.g. parked at an
	// approval gate) must come back up as waiting, not running; a fresh run
	// has no waiting stages yet and starts as running.
	run.Status = model.StatusRunning
	if w := sched.deriveRunStatus(run); w != "" {
		run.Status = w
	}
	run.StartedAt = &now
	run.UpdatedAt = now
	if err := e.st.SaveRun(ctx, run); err != nil {
		return
	}

	for {
		if err := ctx.Err(); err != nil {
			run, _ = e.st.LoadRun(context.Background(), runID)
			if run != nil && !run.Status.IsTerminal() {
				e.markUnfinished(context.Background(), run, model.StatusCanceled, model.StatusCanceled, e.cancelReason(h, "run canceled"))
			}
			return
		}
		select {
		case <-timeoutCh:
			cancel()
			run, _ = e.st.LoadRun(context.Background(), runID)
			e.markUnfinished(context.Background(), run, model.StatusCanceled, model.StatusCanceled, "run timeout exceeded")
			return
		default:
		}

		action, err := sched.tick(ctx)
		if err != nil {
			run, _ = e.st.LoadRun(context.Background(), runID)
			if run != nil && !run.Status.IsTerminal() {
				e.finish(context.Background(), run, model.StatusFailed, "scheduler error: "+err.Error())
			}
			return
		}
		switch action {
		case actionDone:
			return
		case actionWaiting:
			select {
			case <-ctx.Done():
			case <-time.After(200 * time.Millisecond):
			}
		case actionProgress:
		}
	}
}

// acquireSlot blocks until a run concurrency slot is available.
func (e *Engine) acquireSlot(ctx context.Context, h *runHandle) error {
	if e.runConcurrency <= 0 {
		return nil
	}
	e.mu.Lock()
	if e.slots == nil {
		e.slots = make(chan struct{}, e.runConcurrency)
	}
	slots := e.slots
	e.mu.Unlock()

	e.mu.Lock()
	h.blocked = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		h.blocked = false
		e.mu.Unlock()
	}()
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) releaseSlot() {
	if e.runConcurrency <= 0 || e.slots == nil {
		return
	}
	select {
	case <-e.slots:
	default:
	}
}

// cancelReason returns the user-supplied cancel reason recorded on the run
// handle, falling back to a generic reason when the cancellation did not
// originate from CancelRun (engine stop, deadline).
func (e *Engine) cancelReason(h *runHandle, fallback string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if h.reason != "" {
		return h.reason
	}
	return fallback
}

// markUnfinished marks all non-terminal stages/tasks with the given status
// (skipped or canceled) and finalizes the run accordingly.
func (e *Engine) markUnfinished(ctx context.Context, run *model.Run, stageStatus, finalStatus model.Status, reason string) {
	unlock := e.runLock(run.ID)
	defer unlock()
	// Re-read under the lock so we never overwrite a newer stage result.
	fresh, err := e.st.LoadRun(ctx, run.ID)
	if err == nil {
		run = fresh
	}
	// A run that is already final must never be moved again; the first
	// terminal state wins so a concurrent cancel cannot clobber a result.
	if run.Status.IsTerminal() {
		return
	}
	now := time.Now()
	for i := range run.Stages {
		st := &run.Stages[i]
		if st.Status.IsTerminal() {
			continue
		}
		st.Status = stageStatus
		st.Reason = reason
		st.FinishedAt = &now
		for j := range st.Tasks {
			if !st.Tasks[j].Status.IsTerminal() {
				st.Tasks[j].Status = stageStatus
				st.Tasks[j].FinishedAt = &now
			}
		}
		if st.Approval != nil && st.Approval.Status == model.StatusPending {
			st.Approval.Status = model.StatusCanceled
			st.Approval.Comment = reason
			st.Approval.DecidedAt = &now
		}
		e.appr.Cancel(run.ID, st.Name, reason)
	}
	// The run always converges to its final status, even when every stage
	// was already terminal (otherwise it would stay "running" forever).
	run.Status = finalStatus
	run.Reason = reason
	run.FinishedAt = &now
	run.UpdatedAt = now
	_ = e.st.SaveRun(ctx, run)
}

// finish sets the run's final status and persists it under the run lock.
func (e *Engine) finish(ctx context.Context, run *model.Run, status model.Status, reason string) {
	unlock := e.runLock(run.ID)
	defer unlock()
	fresh, err := e.st.LoadRun(ctx, run.ID)
	if err == nil {
		run = fresh
	}
	if run.Status.IsTerminal() {
		return
	}
	now := time.Now()
	run.Status = status
	if reason != "" {
		run.Reason = reason
	}
	run.FinishedAt = &now
	run.UpdatedAt = now
	_ = e.st.SaveRun(ctx, run)
}
