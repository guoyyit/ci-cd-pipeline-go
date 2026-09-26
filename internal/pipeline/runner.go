package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// runHandle 跟踪内存中的活跃运行及其取消函数。
type runHandle struct {
	run         *Run
	ctx         context.Context
	cancel      context.CancelFunc
	semAcquired bool
}

// Runner 是运行编排器：调度、审批、重试、取消、并发控制与恢复。
type Runner struct {
	cfg   *Config
	store *Store
	exec  *Executor

	mu      sync.Mutex
	runs    map[string]*runHandle
	sem     chan struct{}
	notify  chan struct{}
	stopCh  chan struct{}
	stopped chan struct{}
	wg      sync.WaitGroup
	rootCtx context.Context
}

// NewRunner 创建编排器。
func NewRunner(cfg *Config, store *Store, exec *Executor) *Runner {
	return &Runner{
		cfg:     cfg,
		store:   store,
		exec:    exec,
		runs:    map[string]*runHandle{},
		sem:     make(chan struct{}, cfg.Settings.MaxConcurrentRuns),
		notify:  make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

// Start 启动编排循环并执行崩溃恢复。
func (r *Runner) Start(ctx context.Context) {
	r.rootCtx = ctx
	r.recover()
	r.wg.Add(1)
	go r.loop()
}

// Stop 停止编排器，取消全部运行并等待子任务退出。
func (r *Runner) Stop() {
	close(r.stopCh)
	r.mu.Lock()
	for _, h := range r.runs {
		h.cancel()
	}
	r.mu.Unlock()
	<-r.stopped
	r.wg.Wait()
}

// recover 崩溃恢复策略：
//   - 崩溃时处于 running 的阶段/任务标记为 failed（interrupted），可后续重试；
//   - pending/waiting 阶段保持原状，由调度循环继续推进；
//   - 成功阶段绝不重复执行。
func (r *Runner) recover() {
	for _, run := range r.store.IncompleteRuns() {
		changed := false
		for _, sr := range run.Stages {
			if sr.Status == StatusRunning {
				sr.Status = StatusFailed
				sr.Error = "interrupted: service restarted"
				now := time.Now()
				sr.EndedAt = &now
				changed = true
			}
			for _, t := range sr.Tasks {
				if t.Status == StatusRunning {
					t.Status = StatusFailed
					t.Error = "interrupted: service restarted"
					now := time.Now()
					t.EndedAt = &now
					changed = true
				}
			}
		}
		RecomputeRunStatus(run)
		if run.Status.Terminal() {
			// 例如所有阶段本已成功但运行状态未落盘。
			_ = r.store.SaveRun(run)
			continue
		}
		if changed || run.Status != StatusPending {
			_ = r.store.SaveRun(run)
		}
		r.activate(run)
	}
}

// activate 将运行纳入内存调度（持有调用方上下文派生的超时控制）。
func (r *Runner) activate(run *Run) {
	ctx := r.rootCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if d := r.cfg.Settings.RunTimeout.Duration; d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		r.mu.Lock()
		r.runs[run.ID] = &runHandle{run: run, ctx: ctx, cancel: cancel}
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.runs[run.ID] = &runHandle{run: run, ctx: ctx, cancel: cancel}
	r.mu.Unlock()
}

// loop 是编排事件循环。
func (r *Runner) loop() {
	defer r.wg.Done()
	defer close(r.stopped)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.notify:
			r.tick()
		case <-ticker.C:
			r.tick()
		}
	}
}

// wake 非阻塞地唤醒事件循环。
func (r *Runner) wake() {
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

// tick 推进所有活跃运行。
func (r *Runner) tick() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, h := range r.runs {
		run := h.run

		// 运行被取消（手动或超时）：终止所有未完结阶段。
		if h.ctx.Err() != nil {
			cancelOpenStages(run, h.ctx.Err())
		}

		// 审批超时检查。
		checkApprovalTimeouts(run)

		if Evaluate(run, r.cfg) {
			_ = r.store.SaveRun(run)
		}
		RecomputeRunStatus(run)

		if run.Status.Terminal() {
			if h.semAcquired {
				<-r.sem
			}
			h.cancel()
			_ = r.store.SaveRun(run)
			delete(r.runs, id)
			continue
		}

		// 启动可运行阶段（受并发上限约束）。
		runnable := RunnableStages(run, r.cfg)
		if len(runnable) > 0 && !h.semAcquired {
			select {
			case r.sem <- struct{}{}:
				h.semAcquired = true
			default:
				continue // 并发已满，保持排队
			}
		}
		launched := false
		for _, sr := range runnable {
			if sr.Status != StatusPending {
				continue
			}
			now := time.Now()
			sr.Status = StatusRunning
			sr.StartedAt = &now
			if run.StartedAt == nil {
				run.StartedAt = &now
			}
			launched = true
			r.wg.Add(1)
			go r.executeStage(h, sr.Name)
		}
		if launched {
			RecomputeRunStatus(run)
		}
		_ = r.store.SaveRun(run)
	}
}

// cancelOpenStages 将未完结阶段标记为取消（运行被取消/超时时）。
func cancelOpenStages(run *Run, cause error) {
	reason := "cancelled"
	if errors.Is(cause, context.DeadlineExceeded) {
		reason = "run timeout exceeded"
	}
	for _, sr := range run.Stages {
		switch sr.Status {
		case StatusPending, StatusWaiting:
			sr.Status = StatusCancelled
			sr.Error = reason
			now := time.Now()
			if sr.EndedAt == nil {
				sr.EndedAt = &now
			}
			for _, t := range sr.Tasks {
				if t.Status == StatusPending {
					t.Status = StatusCancelled
					t.Error = reason
				}
			}
		}
	}
}

// checkApprovalTimeouts 处理审批超时：超时即取消该阶段。
func checkApprovalTimeouts(run *Run) {
	now := time.Now()
	for _, sr := range run.Stages {
		if sr.Status != StatusWaiting || sr.Approval == nil || sr.Approval.Deadline == nil {
			continue
		}
		if now.Before(*sr.Approval.Deadline) {
			continue
		}
		sr.Approval.Decided = true
		sr.Approval.Approved = false
		sr.Approval.Approver = "system"
		sr.Approval.Reason = "approval timed out"
		sr.Approval.DecidedAt = &now
		sr.Status = StatusCancelled
		sr.Error = "approval timed out"
		sr.EndedAt = &now
		for _, t := range sr.Tasks {
			if t.Status == StatusPending {
				t.Status = StatusSkipped
			}
		}
	}
}

// executeStage 执行单个阶段（含失败自动重试），完成后唤醒调度。
func (r *Runner) executeStage(h *runHandle, stageName string) {
	defer r.wg.Done()
	defer r.wake()

	run := h.run
	st := r.cfg.Stage(stageName)
	sr := run.Stages[stageName]

	ctx := h.ctx
	if st.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, st.Timeout.Duration)
		defer cancel()
	}

	maxAttempts := 1 + st.Retries
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		r.mu.Lock()
		sr.Attempts = attempt
		r.mu.Unlock()

		failed := r.runStageTasks(ctx, run, st, sr)
		if !failed || ctx.Err() != nil {
			break
		}
		if attempt < maxAttempts {
			r.mu.Lock()
			for _, t := range sr.Tasks {
				if t.Status == StatusFailed {
					t.Status = StatusPending
					t.Error = ""
				}
			}
			r.mu.Unlock()
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	sr.EndedAt = &now
	switch {
	case sr.Status == StatusSuccess:
		// 全部任务成功，已在 runStageTasks 中设置。
	case errors.Is(h.ctx.Err(), context.Canceled):
		sr.Status = StatusCancelled
		if sr.Error == "" {
			sr.Error = "cancelled"
		}
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		sr.Status = StatusFailed
		sr.Error = "stage timeout exceeded"
	case sr.Status == StatusRunning:
		// 任务失败已设置失败态；此处兜底。
		sr.Status = StatusFailed
		if sr.Error == "" {
			sr.Error = "stage failed"
		}
	}
	for _, t := range sr.Tasks {
		if t.Status == StatusPending {
			t.Status = StatusSkipped
		}
	}
	_ = r.store.SaveRun(run)
}

// runStageTasks 顺序执行阶段内任务，返回是否失败。
func (r *Runner) runStageTasks(ctx context.Context, run *Run, st *StageConfig, sr *StageRun) bool {
	failed := false
	for i := range st.Tasks {
		tcfg := &st.Tasks[i]
		var tr *TaskRun
		for _, t := range sr.Tasks {
			if t.Name == tcfg.Name {
				tr = t
				break
			}
		}
		if tr == nil || tr.Status == StatusSuccess {
			continue
		}
		if ctx.Err() != nil {
			return true
		}

		r.mu.Lock()
		tr.Status = StatusRunning
		now := time.Now()
		tr.StartedAt = &now
		tr.Attempts++
		_ = r.store.SaveRun(run)
		r.mu.Unlock()

		res := r.exec.RunTask(ctx, run.ID, st.Name, *tcfg)

		r.mu.Lock()
		end := time.Now()
		tr.EndedAt = &end
		tr.LogPath = res.LogPath
		tr.ExitCode = res.ExitCode
		switch {
		case errors.Is(res.Err, context.Canceled):
			tr.Status = StatusCancelled
			tr.Error = "cancelled"
			failed = true
		case errors.Is(res.Err, context.DeadlineExceeded):
			tr.Status = StatusFailed
			tr.Error = "task timeout exceeded"
			failed = true
		case res.Err != nil || res.ExitCode != 0:
			tr.Status = StatusFailed
			if res.Err != nil {
				tr.Error = res.Err.Error()
			} else {
				tr.Error = fmt.Sprintf("exit code %d", res.ExitCode)
			}
			failed = true
		default:
			tr.Status = StatusSuccess
		}
		_ = r.store.SaveRun(run)
		r.mu.Unlock()

		if failed {
			return true
		}
	}
	r.mu.Lock()
	sr.Status = StatusSuccess
	_ = r.store.SaveRun(run)
	r.mu.Unlock()
	return false
}

// SubmitEvent 处理触发事件；重复事件返回已存在运行且 created=false。
func (r *Runner) SubmitEvent(e Event) (*Run, bool, error) {
	if e.ReceivedAt.IsZero() {
		e.ReceivedAt = time.Now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.store.HasEvent(e.Key()) {
		for _, existing := range r.store.ListRuns() {
			if existing.Event.Key() == e.Key() {
				return existing, false, nil
			}
		}
		return nil, false, nil
	}
	if err := r.store.RecordEvent(e); err != nil {
		return nil, false, err
	}

	// 触发过滤：分支/标签规则。
	matched := false
	switch e.Type {
	case EventPush:
		matched = MatchAny(r.cfg.Trigger.Branches, e.Ref)
	case EventTag:
		matched = MatchAny(r.cfg.Trigger.Tags, e.Ref)
	}
	if !matched {
		return nil, false, nil
	}

	run := NewRun(r.cfg, e, newRunID())
	if err := r.store.SaveRun(run); err != nil {
		return nil, false, err
	}
	r.activateLocked(run)
	r.wake()
	return run, true, nil
}

// activateLocked 与 activate 相同，但调用方已持有锁。
func (r *Runner) activateLocked(run *Run) {
	ctx := r.rootCtx
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc
	if d := r.cfg.Settings.RunTimeout.Duration; d > 0 {
		ctx, cancel = context.WithTimeout(ctx, d)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	r.runs[run.ID] = &runHandle{run: run, ctx: ctx, cancel: cancel}
}

// CancelRun 取消运行并传播到正在执行的子任务。
func (r *Runner) CancelRun(id string) error {
	r.mu.Lock()
	h, ok := r.runs[id]
	r.mu.Unlock()
	if !ok {
		if run, found := r.store.GetRun(id); found && run.Status.Terminal() {
			return fmt.Errorf("run %s already finished with status %s", id, run.Status)
		}
		return fmt.Errorf("run %s not found", id)
	}
	h.cancel()
	r.wake()
	return nil
}

// RetryRun 重试失败及下游阶段；force 时全量重跑。
func (r *Runner) RetryRun(id string, force bool) error {
	r.mu.Lock()
	if _, active := r.runs[id]; active {
		r.mu.Unlock()
		return fmt.Errorf("run %s is still active", id)
	}
	run, ok := r.store.GetRun(id)
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("run %s not found", id)
	}
	if !run.Status.Terminal() {
		r.mu.Unlock()
		return fmt.Errorf("run %s is not finished", id)
	}
	ResetForRetry(run, r.cfg, force)
	if err := r.store.SaveRun(run); err != nil {
		r.mu.Unlock()
		return err
	}
	r.activateLocked(run)
	r.mu.Unlock()
	r.wake()
	return nil
}

// Approve 通过某阶段的人工审批。
func (r *Runner) Approve(runID, stage, approver string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.runs[runID]
	if !ok {
		return fmt.Errorf("run %s not active", runID)
	}
	sr, ok := h.run.Stages[stage]
	if !ok {
		return fmt.Errorf("stage %s not found", stage)
	}
	if sr.Status != StatusWaiting || sr.Approval == nil {
		return fmt.Errorf("stage %s is not waiting for approval", stage)
	}
	now := time.Now()
	sr.Approval.Decided = true
	sr.Approval.Approved = true
	sr.Approval.Approver = approver
	sr.Approval.DecidedAt = &now
	sr.Status = StatusPending
	if err := r.store.SaveRun(h.run); err != nil {
		return err
	}
	r.wake()
	return nil
}

// Reject 拒绝某阶段的人工审批；必须给出理由以保证可追溯。
func (r *Runner) Reject(runID, stage, approver, reason string) error {
	if reason == "" {
		return fmt.Errorf("reject reason is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.runs[runID]
	if !ok {
		return fmt.Errorf("run %s not active", runID)
	}
	sr, ok := h.run.Stages[stage]
	if !ok {
		return fmt.Errorf("stage %s not found", stage)
	}
	if sr.Status != StatusWaiting || sr.Approval == nil {
		return fmt.Errorf("stage %s is not waiting for approval", stage)
	}
	now := time.Now()
	sr.Approval.Decided = true
	sr.Approval.Approved = false
	sr.Approval.Approver = approver
	sr.Approval.Reason = reason
	sr.Approval.DecidedAt = &now
	sr.Status = StatusCancelled
	sr.Error = fmt.Sprintf("rejected by %s: %s", approver, reason)
	sr.EndedAt = &now
	for _, t := range sr.Tasks {
		if t.Status == StatusPending {
			t.Status = StatusSkipped
		}
	}
	if err := r.store.SaveRun(h.run); err != nil {
		return err
	}
	r.wake()
	return nil
}

// GetRun 查询运行（优先内存中的活跃运行）。
func (r *Runner) GetRun(id string) (*Run, bool) {
	r.mu.Lock()
	if h, ok := r.runs[id]; ok {
		cp := deepCopyRun(h.run)
		r.mu.Unlock()
		return cp, true
	}
	r.mu.Unlock()
	return r.store.GetRun(id)
}

// ListRuns 列出全部运行。
func (r *Runner) ListRuns() []*Run {
	return r.store.ListRuns()
}

// newRunID 生成唯一运行 ID。
func newRunID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("run-%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}
