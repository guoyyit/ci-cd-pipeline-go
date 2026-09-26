package pipeline

import (
	"fmt"
	"time"
)

// NewRun 依据配置与事件创建一次新运行。
func NewRun(cfg *Config, e Event, id string) *Run {
	now := time.Now()
	run := &Run{
		ID:         id,
		Pipeline:   cfg.Name,
		Event:      e,
		Status:     StatusPending,
		Stages:     map[string]*StageRun{},
		StageOrder: cfg.StageNames(),
		Attempt:    1,
		CreatedAt:  now,
	}
	for _, st := range cfg.Stages {
		sr := &StageRun{Name: st.Name, Status: StatusPending}
		if st.Approval != nil {
			sr.Approval = &ApprovalState{Required: true}
		}
		for _, t := range st.Tasks {
			sr.Tasks = append(sr.Tasks, &TaskRun{Name: t.Name, Status: StatusPending})
		}
		run.Stages[st.Name] = sr
	}
	return run
}

// depsOf 返回阶段依赖的运行状态。
func depsOf(run *Run, cfg *Config, name string) []*StageRun {
	st := cfg.Stage(name)
	if st == nil {
		return nil
	}
	var deps []*StageRun
	for _, dep := range st.Needs {
		if d, ok := run.Stages[dep]; ok {
			deps = append(deps, d)
		}
	}
	return deps
}

// Evaluate 推进运行中各阶段状态，返回是否有变化：
//   - 依赖全部成功且需要审批 → 进入 waiting 并设置审批截止；
//   - 任一依赖失败/取消/跳过 → 本阶段跳过。
func Evaluate(run *Run, cfg *Config) bool {
	changed := false
	for _, name := range cfg.TopoStages() {
		sr := run.Stages[name]
		if sr == nil || sr.Status != StatusPending {
			continue
		}
		deps := depsOf(run, cfg, name)
		blocked := false
		allOK := true
		for _, d := range deps {
			if !d.Status.Terminal() {
				allOK = false
				break
			}
			if d.Status != StatusSuccess {
				blocked = true
			}
		}
		if !allOK {
			continue
		}
		if blocked {
			markSkipped(sr)
			changed = true
			continue
		}
		if sr.Approval != nil && !sr.Approval.Decided {
			sr.Status = StatusWaiting
			if sr.Approval.Deadline == nil {
				st := cfg.Stage(name)
				if st != nil && st.Approval.Timeout.Duration > 0 {
					dl := time.Now().Add(st.Approval.Timeout.Duration)
					sr.Approval.Deadline = &dl
				}
			}
			changed = true
		}
	}
	return changed
}

// markSkipped 将阶段及其未开始的任务标记为跳过。
func markSkipped(sr *StageRun) {
	sr.Status = StatusSkipped
	now := time.Now()
	if sr.EndedAt == nil {
		sr.EndedAt = &now
	}
	for _, t := range sr.Tasks {
		if t.Status == StatusPending {
			t.Status = StatusSkipped
		}
	}
}

// RunnableStages 返回当前可启动的阶段：
// 状态为 pending、依赖全部成功、且审批已通过（或无需审批）。
func RunnableStages(run *Run, cfg *Config) []*StageRun {
	var out []*StageRun
	for _, name := range run.StageOrder {
		sr := run.Stages[name]
		if sr == nil || sr.Status != StatusPending {
			continue
		}
		allOK := true
		for _, d := range depsOf(run, cfg, name) {
			if d.Status != StatusSuccess {
				allOK = false
				break
			}
		}
		if !allOK {
			continue
		}
		if sr.Approval != nil && !(sr.Approval.Decided && sr.Approval.Approved) {
			continue
		}
		out = append(out, sr)
	}
	return out
}

// DownstreamStages 返回某阶段的全部下游阶段名（传递闭包）。
func DownstreamStages(cfg *Config, name string) map[string]bool {
	children := map[string][]string{}
	for _, st := range cfg.Stages {
		for _, dep := range st.Needs {
			children[dep] = append(children[dep], st.Name)
		}
	}
	out := map[string]bool{}
	queue := []string{name}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, ch := range children[n] {
			if !out[ch] {
				out[ch] = true
				queue = append(queue, ch)
			}
		}
	}
	return out
}

// ResetForRetry 为重试重置失败/取消阶段及其下游；force 时全量重置。
// 成功阶段在非强制模式下保持原状，绝不重复执行。
func ResetForRetry(run *Run, cfg *Config, force bool) {
	reset := map[string]bool{}
	if force {
		for name := range run.Stages {
			reset[name] = true
		}
	} else {
		queue := []string{}
		for name, sr := range run.Stages {
			if sr.Status == StatusFailed || sr.Status == StatusCancelled {
				queue = append(queue, name)
			}
		}
		for i := 0; i < len(queue); i++ {
			n := queue[i]
			if reset[n] {
				continue
			}
			reset[n] = true
			for d := range DownstreamStages(cfg, n) {
				queue = append(queue, d)
			}
		}
	}
	for name := range reset {
		sr := run.Stages[name]
		if sr == nil {
			continue
		}
		resetStage(sr)
	}
	run.Status = StatusPending
	run.EndedAt = nil
	run.Error = ""
	run.Attempt++
}

// resetStage 将阶段重置为待调度状态。
func resetStage(sr *StageRun) {
	sr.Status = StatusPending
	sr.StartedAt = nil
	sr.EndedAt = nil
	sr.Error = ""
	if sr.Approval != nil {
		sr.Approval = &ApprovalState{Required: true}
	}
	for _, t := range sr.Tasks {
		t.Status = StatusPending
		t.ExitCode = 0
		t.StartedAt = nil
		t.EndedAt = nil
		t.Error = ""
	}
}

// RecomputeRunStatus 依据阶段状态重算运行状态与时间戳。
func RecomputeRunStatus(run *Run) {
	allTerminal := true
	anyFailed := false
	anyCancelled := false
	anyActive := false
	for _, name := range run.StageOrder {
		sr := run.Stages[name]
		if sr == nil {
			continue
		}
		switch {
		case !sr.Status.Terminal():
			allTerminal = false
			if sr.Status == StatusRunning || sr.Status == StatusWaiting {
				anyActive = true
			}
		case sr.Status == StatusFailed:
			anyFailed = true
		case sr.Status == StatusCancelled:
			anyCancelled = true
		}
	}
	now := time.Now()
	switch {
	case !allTerminal:
		if anyActive || run.StartedAt != nil {
			if run.Status != StatusRunning {
				run.Status = StatusRunning
			}
		} else {
			run.Status = StatusPending
		}
		run.EndedAt = nil
	default:
		switch {
		case anyFailed:
			run.Status = StatusFailed
		case anyCancelled:
			run.Status = StatusCancelled
		default:
			run.Status = StatusSuccess
		}
		if run.EndedAt == nil {
			run.EndedAt = &now
		}
	}
}

// String 便于调试输出。
func (r *Run) String() string {
	return fmt.Sprintf("run %s (%s) status=%s attempt=%d", r.ID, r.Pipeline, r.Status, r.Attempt)
}
