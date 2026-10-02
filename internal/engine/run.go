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

	// Bind to the handle pre-registered by TriggerEvent/launch so a cancel
	// that arrived before this goroutine scheduled is already captured.
	var ctx context.Context
	e.mu.Lock()
	h, ok := e.active[runID]
	if !ok {
		rootCtx := e.rootCtx
		if rootCtx == nil {
			rootCtx = context.Background()
		}
		runCtx, cancel := context.WithCancel(rootCtx)
		h = &runHandle{id: runID, ctx: runCtx, cancel: cancel, release: cancel}
		e.active[runID] = h
		ctx = runCtx
	}
	ready := h.ready
	e.mu.Unlock()
	// Close readiness before any I/O or run-lock acquisition: CancelRun may
	// be holding the run lock while waiting for this signal, so doing it
	// under e.mu only (above) prevents a lock-order cycle.
	if ready != nil {
		close(ready)
	}
	if ctx == nil {
		ctx = h.ctx
	}
	defer h.release()
	defer func() {
		e.mu.Lock()
		delete(e.active, runID)
		e.mu.Unlock()
	}()

	run, err := e.st.LoadRun(ctx, runID)
	if err != nil {
		return
	}
	// A cancel accepted before the loop started already persisted the
	// terminal state; honor it immediately and start nothing.
	if run.Status.IsTerminal() {
		return
	}
	var timeoutCh <-chan time.Time
	if run.Deadline != nil {
		d := time.Until(*run.Deadline)
		if d <= 0 {
			e.markUnfinished(context.Background(), run, model.StatusCanceled, model.StatusCanceled, "run deadline already exceeded at start")
			return
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timeoutCh = t.C
	}

	if err := e.acquireSlot(ctx, h); err != nil {
		run, _ = e.st.LoadRun(context.Background(), runID)
		if run != nil && !run.Status.IsTerminal() {
			e.markUnfinished(context.Background(), run, model.StatusCanceled, model.StatusCanceled, "engine stopped before run started")
		}
		return
	}
	defer e.releaseSlot()

	run, err = e.st.LoadRun(ctx, runID)
	if err != nil {
		return
	}
	if run.Status.IsTerminal() {
		return
	}

	// Stamp the start time once; the run status is derived from its stages
	// (waiting while blocked, running while executing) on every scheduler
	// tick, so a blocked run is never reported as running.
	var changed bool
	if run.StartedAt == nil {
		now := time.Now()
		run.StartedAt = &now
		run.UpdatedAt = now
		changed = true
	}
	if changed {
		if err := e.st.SaveRun(ctx, run); err != nil {
			return
		}
	}

	sched := newScheduler(e, run)

	finishWith := func(status model.Status, reason string) {
		fresh, _ := e.st.LoadRun(context.Background(), runID)
		if fresh != nil && !fresh.Status.IsTerminal() {
			e.markUnfinished(context.Background(), fresh, status, status, reason)
		}
	}
	for {
		select {
		case <-timeoutCh:
			// Signal the run context so any executing child is killed and
			// reaped, then persist the terminal state.
			h.cancel()
			finishWith(model.StatusCanceled, "run timeout exceeded")
			return
		case <-ctx.Done():
			// Engine stop (the user-cancel path persisted the terminal
			// state itself and would not reach this branch).
			finishWith(model.StatusCanceled, "run canceled")
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
		}
		// Yield before the next tick so timeout/cancel are observed
		// promptly when the scheduler is blocked without making progress.
		var delay time.Duration
		if action == actionWaiting {
			delay = 200 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timeoutCh:
			timer.Stop()
			h.cancel()
			finishWith(model.StatusCanceled, "run timeout exceeded")
			return
		case <-timer.C:
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
	for i := range run.Stages {
		st := &run.Stages[i]
		if st.Status.IsTerminal() {
			continue
		}
		st.Status = stageStatus
		st.Reason = reason
		now := time.Now()
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
		if st.Approval != nil {
			e.appr.Cancel(run.ID, st.Name, reason)
		}
	}
	// Even when every stage already reached a terminal state (e.g. a worker
	// canceled its own stage concurrently), the run itself must be finalized
	// here, otherwise it could stay running/waiting forever.
	now := time.Now()
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
	now := time.Now()
	run.Status = status
	if reason != "" {
		run.Reason = reason
	}
	run.FinishedAt = &now
	run.UpdatedAt = now
	_ = e.st.SaveRun(ctx, run)
}
