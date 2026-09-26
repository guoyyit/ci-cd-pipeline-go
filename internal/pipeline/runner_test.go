package pipeline

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// newTestRunner 构造使用临时目录的 Runner。
func newTestRunner(t *testing.T, cfg *Config) (*Runner, *Store, context.CancelFunc) {
	t.Helper()
	dir := t.TempDir()
	cfg.Settings.DataDir = dir
	if cfg.Settings.MaxConcurrentRuns == 0 {
		cfg.Settings.MaxConcurrentRuns = 2
	}
	if cfg.Settings.RunTimeout.Duration == 0 {
		cfg.Settings.RunTimeout = Duration{time.Minute}
	}
	if cfg.Repo.Path == "" {
		cfg.Repo.Path = dir
	}
	store, err := OpenStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	exec := NewExecutor(dir, dir)
	r := NewRunner(cfg, store, exec)
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)
	t.Cleanup(func() {
		cancel()
		r.Stop()
		store.Close()
	})
	return r, store, cancel
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

func runStatus(t *testing.T, r *Runner, id string) Status {
	t.Helper()
	run, ok := r.GetRun(id)
	if !ok {
		t.Fatalf("run %s not found", id)
	}
	return run.Status
}

func simpleCfg(stages ...StageConfig) *Config {
	return &Config{
		Name:    "it",
		Trigger: TriggerConfig{Branches: []string{"*"}, Tags: []string{"v*"}},
		Stages:  stages,
	}
}

func TestDuplicateEventCreatesSingleRun(t *testing.T) {
	cfg := simpleCfg(StageConfig{Name: "a", Tasks: []TaskConfig{{Name: "t", Run: "true"}}})
	r, _, _ := newTestRunner(t, cfg)

	e := Event{Type: EventPush, Repo: "repo", Ref: "main", Commit: "abc"}
	run1, created1, err := r.SubmitEvent(e)
	if err != nil || !created1 {
		t.Fatalf("first submit created=%v err=%v", created1, err)
	}
	run2, created2, err := r.SubmitEvent(e)
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Fatal("duplicate event must not create a new run")
	}
	if run2 == nil || run2.ID != run1.ID {
		t.Fatalf("duplicate event should return existing run %s, got %+v", run1.ID, run2)
	}
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run1.ID) == StatusSuccess
	}, "run success")
	if n := len(r.ListRuns()); n != 1 {
		t.Fatalf("runs = %d, want 1", n)
	}
}

func TestTriggerFiltering(t *testing.T) {
	cfg := simpleCfg(StageConfig{Name: "a", Tasks: []TaskConfig{{Name: "t", Run: "true"}}})
	cfg.Trigger.Branches = []string{"main"}
	r, _, _ := newTestRunner(t, cfg)

	run, created, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "dev", Commit: "1"})
	if created || run != nil {
		t.Fatal("non-matching branch must not trigger")
	}
	if _, created, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "2"}); !created {
		t.Fatal("matching branch should trigger")
	}
	if _, created, _ := r.SubmitEvent(Event{Type: EventTag, Repo: "r", Ref: "v1.0", Commit: "3"}); !created {
		t.Fatal("matching tag should trigger")
	}
	if _, created, _ := r.SubmitEvent(Event{Type: EventTag, Repo: "r", Ref: "nightly", Commit: "4"}); created {
		t.Fatal("non-matching tag must not trigger")
	}
}

func TestCancelPropagatesToTasks(t *testing.T) {
	cfg := simpleCfg(StageConfig{
		Name: "slow",
		Tasks: []TaskConfig{
			{Name: "t", Run: "sleep 30"},
			{Name: "never", Run: "true"},
		},
	})
	r, _, _ := newTestRunner(t, cfg)
	run, _, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "c1"})

	waitFor(t, 5*time.Second, func() bool {
		got, _ := r.GetRun(run.ID)
		return got.Stages["slow"].Tasks[0].Status == StatusRunning
	}, "task running")

	if err := r.CancelRun(run.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run.ID) == StatusCancelled
	}, "run cancelled")

	got, _ := r.GetRun(run.ID)
	if got.Stages["slow"].Tasks[0].Status != StatusCancelled {
		t.Fatalf("task0 = %s, want cancelled", got.Stages["slow"].Tasks[0].Status)
	}
	// 取消后进程应被杀死：sleep 30 不可能还在运行
	if err := exec.Command("pgrep", "-f", "sleep 30").Run(); err == nil {
		t.Fatal("leftover sleep process after cancel")
	}
}

func TestRetryRunsOnlyFailedAndDownstream(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "build-ran")
	cfg := simpleCfg(
		StageConfig{Name: "build", Tasks: []TaskConfig{{Name: "t", Run: "echo ok >> " + marker}}},
		StageConfig{Name: "test", Needs: []string{"build"}, Tasks: []TaskConfig{{Name: "t", Run: "false"}}},
		StageConfig{Name: "pack", Needs: []string{"test"}, Tasks: []TaskConfig{{Name: "t", Run: "true"}}},
	)
	r, _, _ := newTestRunner(t, cfg)
	run, _, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "c1"})
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run.ID) == StatusFailed
	}, "run failed")

	got, _ := r.GetRun(run.ID)
	if got.Stages["pack"].Status != StatusSkipped {
		t.Fatalf("pack = %s, want skipped", got.Stages["pack"].Status)
	}

	// 修复失败命令后重试（修改配置中的任务命令）
	cfg.Stages[1].Tasks[0].Run = "true"
	if err := r.RetryRun(run.ID, false); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run.ID) == StatusSuccess
	}, "run success after retry")

	// build 成功阶段不得重复执行：marker 文件应只有一行
	data, _ := os.ReadFile(marker)
	if string(data) != "ok\n" {
		t.Fatalf("build re-executed, marker=%q", data)
	}
	got, _ = r.GetRun(run.ID)
	if got.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", got.Attempt)
	}
}

func TestForceRetryRerunsAll(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "count")
	cfg := simpleCfg(StageConfig{Name: "a", Tasks: []TaskConfig{{Name: "t", Run: "echo x >> " + marker}}})
	r, _, _ := newTestRunner(t, cfg)
	run, _, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "c1"})
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run.ID) == StatusSuccess
	}, "run success")

	if err := r.RetryRun(run.ID, true); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		data, _ := os.ReadFile(marker)
		return len(data) == 4 // "x\nx\n"
	}, "force rerun executed task twice")
}

func TestApprovalFlow(t *testing.T) {
	cfg := simpleCfg(
		StageConfig{Name: "build", Tasks: []TaskConfig{{Name: "t", Run: "true"}}},
		StageConfig{Name: "deploy", Needs: []string{"build"},
			Approval: &ApprovalConfig{Timeout: Duration{time.Hour}},
			Tasks:    []TaskConfig{{Name: "t", Run: "true"}}},
	)
	r, _, _ := newTestRunner(t, cfg)
	run, _, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "c1"})

	waitFor(t, 5*time.Second, func() bool {
		got, _ := r.GetRun(run.ID)
		return got.Stages["deploy"].Status == StatusWaiting
	}, "deploy waiting for approval")

	// 拒绝需要理由
	if err := r.Reject(run.ID, "deploy", "bob", ""); err == nil {
		t.Fatal("reject without reason must fail")
	}
	if err := r.Approve(run.ID, "deploy", "alice"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run.ID) == StatusSuccess
	}, "run success after approval")

	got, _ := r.GetRun(run.ID)
	ap := got.Stages["deploy"].Approval
	if !ap.Approved || ap.Approver != "alice" || ap.DecidedAt == nil {
		t.Fatalf("approval not traceable: %+v", ap)
	}
}

func TestApprovalRejectTraceable(t *testing.T) {
	cfg := simpleCfg(
		StageConfig{Name: "build", Tasks: []TaskConfig{{Name: "t", Run: "true"}}},
		StageConfig{Name: "deploy", Needs: []string{"build"},
			Approval: &ApprovalConfig{Timeout: Duration{time.Hour}},
			Tasks:    []TaskConfig{{Name: "t", Run: "true"}}},
		StageConfig{Name: "after", Needs: []string{"deploy"}, Tasks: []TaskConfig{{Name: "t", Run: "true"}}},
	)
	r, _, _ := newTestRunner(t, cfg)
	run, _, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "c1"})
	waitFor(t, 5*time.Second, func() bool {
		got, _ := r.GetRun(run.ID)
		return got.Stages["deploy"].Status == StatusWaiting
	}, "waiting")

	if err := r.Reject(run.ID, "deploy", "bob", "not ready"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run.ID) == StatusCancelled
	}, "run cancelled after reject")

	got, _ := r.GetRun(run.ID)
	dep := got.Stages["deploy"]
	if dep.Status != StatusCancelled || dep.Approval.Approver != "bob" || dep.Approval.Reason != "not ready" {
		t.Fatalf("reject not traceable: %+v", dep.Approval)
	}
	if got.Stages["after"].Status != StatusSkipped {
		t.Fatalf("downstream = %s, want skipped", got.Stages["after"].Status)
	}
}

func TestApprovalTimeout(t *testing.T) {
	cfg := simpleCfg(
		StageConfig{Name: "deploy",
			Approval: &ApprovalConfig{Timeout: Duration{200 * time.Millisecond}},
			Tasks:    []TaskConfig{{Name: "t", Run: "true"}}},
	)
	r, _, _ := newTestRunner(t, cfg)
	run, _, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "c1"})
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run.ID) == StatusCancelled
	}, "run cancelled by approval timeout")
	got, _ := r.GetRun(run.ID)
	if got.Stages["deploy"].Approval.Reason != "approval timed out" {
		t.Fatalf("reason = %q", got.Stages["deploy"].Approval.Reason)
	}
}

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := simpleCfg(
		StageConfig{Name: "a", Tasks: []TaskConfig{{Name: "t", Run: "true"}}},
		StageConfig{Name: "b", Needs: []string{"a"}, Tasks: []TaskConfig{{Name: "t", Run: "true"}}},
	)
	cfg.Settings.DataDir = dir
	cfg.Settings.MaxConcurrentRuns = 2
	cfg.Settings.RunTimeout = Duration{time.Minute}
	cfg.Repo.Path = dir

	store, err := OpenStore(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	// 模拟崩溃前状态：a 成功，b 正在运行
	run := NewRun(cfg, Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "c1"}, "run-crash")
	run.Status = StatusRunning
	run.Stages["a"].Status = StatusSuccess
	run.Stages["b"].Status = StatusRunning
	run.Stages["b"].Tasks[0].Status = StatusRunning
	if err := store.SaveRun(run); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordEvent(run.Event); err != nil {
		t.Fatal(err)
	}

	// 重启：新 Runner 从同一存储恢复
	r := NewRunner(cfg, store, NewExecutor(dir, dir))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	defer r.Stop()

	waitFor(t, 5*time.Second, func() bool {
		got, ok := r.GetRun("run-crash")
		return ok && got.Status.Terminal()
	}, "recovered run reaches terminal state")

	got, _ := r.GetRun("run-crash")
	if got.Stages["a"].Status != StatusSuccess {
		t.Fatalf("successful stage must be preserved, got %s", got.Stages["a"].Status)
	}
	if got.Stages["b"].Status != StatusFailed || got.Stages["b"].Error == "" {
		t.Fatalf("interrupted stage = %s err=%q, want failed(interrupted)",
			got.Stages["b"].Status, got.Stages["b"].Error)
	}

	// 恢复后可重试，且 a 不重复执行
	if err := r.RetryRun("run-crash", false); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, "run-crash") == StatusSuccess
	}, "recovered run succeeds after retry")
}
