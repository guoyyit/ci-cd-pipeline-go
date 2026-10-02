package engine

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/git"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

// ---- git repo helpers ------------------------------------------------------

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, dir, err, out)
	}
}

// newGitRepo creates a real git repository with one initial commit on main.
func newGitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	runGit(t, dir, "init", "-q", "-b", "main")
	runGit(t, dir, "config", "user.email", "t@example.com")
	runGit(t, dir, "config", "user.name", "tester")
	runGit(t, dir, "config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "add", ".")
	runGit(t, dir, "commit", "-q", "-m", "initial")
	return dir
}

func gitCommit(t *testing.T, dir, msg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "f-"+msg), []byte(msg), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "add", ".")
	runGit(t, dir, "commit", "-q", "-m", msg)
}

// ---- startup baseline ------------------------------------------------------

// TestFirstScanBaselineProducesNoRuns wires a real git watcher to the engine
// exactly like pipelined does, starts it on a repository that already has
// commits and tags, and asserts that the first scan only establishes the
// baseline: zero events are emitted and zero runs are created. Only a commit
// made after the start may trigger a run.
func TestFirstScanBaselineProducesNoRuns(t *testing.T) {
	repo := newGitRepo(t)
	gitCommit(t, repo, "historical-1")
	gitCommit(t, repo, "historical-2")
	runGit(t, repo, "tag", "v0.9.0")

	eng, _ := newTestEngine(t, 0)
	cfg := &model.Config{
		Name:     "p",
		Repo:     repo,
		Triggers: model.Triggers{Branches: []string{"main"}, Tags: []string{"v*"}},
		Stages:   []model.Stage{{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan model.Event, 16)
	w := git.NewWatcher(repo, 30*time.Millisecond, nil)
	go func() { _ = w.Run(ctx, events) }()
	emitted := make(chan model.Event, 16)
	go func() {
		for ev := range events {
			emitted <- ev
			_, _, _ = eng.TriggerEvent(ctx, cfg, ev)
		}
	}()

	// For several poll intervals nothing historical may be emitted.
	select {
	case ev := <-emitted:
		t.Fatalf("first scan emitted a historical event: %+v", ev)
	case <-time.After(400 * time.Millisecond):
	}
	assertRunCount(t, eng, 0)

	// A commit created after startup triggers exactly one run.
	gitCommit(t, repo, "after-start")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runs, _ := eng.GetRuns(context.Background())
		if len(runs) == 1 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("commit created after startup did not trigger a run")
}

func assertRunCount(t *testing.T, eng *Engine, want int) {
	t.Helper()
	runs, err := eng.GetRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != want {
		t.Fatalf("run count = %d, want %d", len(runs), want)
	}
}

// waitRunStatus polls until the run reaches the wanted (non-terminal) status.
func waitRunStatus(t *testing.T, eng *Engine, id string, want model.Status, timeout time.Duration) *model.Run {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := eng.GetRun(context.Background(), id)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if run.Status == want {
			return run
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach %q within %v", id, want, timeout)
	return nil
}

// ---- waiting status --------------------------------------------------------

// TestWaitingStatusVisibleAndPersisted asserts that a stage parked at an
// approval gate surfaces "waiting" (not pending/running) on both the stage
// and the run, and that the waiting state is persisted to the store.
func TestWaitingStatusVisibleAndPersisted(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "build", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{
			Name: "deploy", Needs: []string{"build"},
			Approval: &model.ApprovalRule{Required: true},
			Tasks:    []model.Task{shellTask("ok", "true")},
		},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("wait-visible"))

	// The deploy stage parks at the approval gate (pending approval recorded).
	waitApprovalPending(t, eng, id, "deploy", 3*time.Second)
	// The run itself surfaces waiting (it is not executing anything).
	waitRunStatus(t, eng, id, model.StatusWaiting, 3*time.Second)

	// The persisted record (a fresh load from the store) shows waiting too.
	run, err := eng.GetRun(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != model.StatusWaiting {
		t.Fatalf("persisted run status = %s, want waiting", run.Status)
	}
	dep := run.Stage("deploy")
	if dep == nil || dep.Status != model.StatusWaiting {
		t.Fatalf("persisted deploy stage = %+v, want waiting", dep)
	}
	if dep.Approval == nil || dep.Approval.Status != model.StatusPending {
		t.Fatalf("approval not pending: %+v", dep.Approval)
	}
}

// waitApprovalPending polls until the named stage has a pending approval.
func waitApprovalPending(t *testing.T, eng *Engine, id, stage string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := eng.GetRun(context.Background(), id)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if st := run.Stage(stage); st != nil && st.Approval != nil &&
			st.Approval.Status == model.StatusPending && st.Status == model.StatusWaiting {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("stage %s did not reach a pending approval within %v", stage, timeout)
}

// waitNoChildren polls until no child processes remain tracked by the engine.
// A canceled run is persisted before its stage workers finish reaping their
// children, so the test must wait for the workers to drain.
func waitNoChildren(t *testing.T, eng *Engine, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if eng.ActiveChildren() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%d child processes still running after %v", eng.ActiveChildren(), timeout)
}

// TestWaitingOnUpstreamStageVisible asserts that a stage blocked on an
// upstream stage is shown as waiting while the upstream stage executes.
func TestWaitingOnUpstreamStageVisible(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "slow", Tasks: []model.Task{shellTask("s", "sleep 1")}},
		model.Stage{Name: "down", Needs: []string{"slow"}, Tasks: []model.Task{shellTask("ok", "true")}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("wait-upstream"))
	waitStageStatus(t, eng, id, "slow", model.StatusRunning, 3*time.Second)
	// While the upstream stage runs, the downstream stage waits on it.
	waitStageStatus(t, eng, id, "down", model.StatusWaiting, 3*time.Second)
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", run.Status, run.Reason)
	}
}

// waitingRunFixture builds an on-disk run parked at an approval gate,
// simulating the persisted state of a daemon that stopped while waiting.
func waitingRunFixture(cfg *model.Config, ev model.Event) *model.Run {
	now := time.Now()
	run := &model.Run{
		ID:         newID("run"),
		Pipeline:   cfg.Name,
		EventID:    ev.ID,
		ConfigJSON: snapshotConfig(cfg),
		Branch:     ev.Branch,
		Commit:     ev.Commit,
		Status:     model.StatusWaiting,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	for _, st := range cfg.Stages {
		ss := model.StageState{Name: st.Name, Group: st.Group, Status: model.StatusWaiting}
		if st.Approval != nil && st.Approval.Required {
			ss.Approval = &model.ApprovalState{Required: true, Status: model.StatusPending, CreatedAt: now}
		}
		for _, task := range st.Tasks {
			ss.Tasks = append(ss.Tasks, model.TaskState{Name: task.Name, Status: model.StatusPending})
		}
		run.Stages = append(run.Stages, ss)
	}
	return run
}

// TestWaitingStatusSurvivesRestart persists a waiting run, then recovers it
// with a brand-new engine (process restart). Recovery must not misclassify
// the waiting run as running: it resumes waiting at the same approval gate,
// the approval stays pending, and approving afterwards completes the run.
func TestWaitingStatusSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := triggerCfg(dir,
		model.Stage{
			Name:     "deploy",
			Approval: &model.ApprovalRule{Required: true},
			Tasks:    []model.Task{shellTask("ok", "true")},
		},
	)
	run := waitingRunFixture(cfg, pushEvent("wait-restart"))
	if err := st.SaveRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}

	eng := New(Options{Store: st, WorkDir: dir})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	ids, err := eng.Recover(context.Background(), RecoveryResume)
	if err != nil || len(ids) != 1 || ids[0] != run.ID {
		t.Fatalf("Recover = %v, %v", ids, err)
	}

	// The run converges back to waiting at the approval gate.
	waitRunStatus(t, eng, run.ID, model.StatusWaiting, 3*time.Second)
	waitStageStatus(t, eng, run.ID, "deploy", model.StatusWaiting, 3*time.Second)
	got, err := eng.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	dep := got.Stage("deploy")
	if dep.Approval == nil || dep.Approval.Status != model.StatusPending {
		t.Fatalf("approval not pending after recovery: %+v", dep.Approval)
	}

	// Approving after the restart still works and completes the run.
	if _, err := eng.Approve(context.Background(), run.ID, "deploy", "alice"); err != nil {
		t.Fatalf("Approve after restart: %v", err)
	}
	final := waitTerminal(t, eng, run.ID, 5*time.Second)
	if final.Status != model.StatusSuccess {
		t.Fatalf("status after restart+approve = %s (%s)", final.Status, final.Reason)
	}
}

// ---- dedup across restarts -------------------------------------------------

// TestDuplicateEventAfterRestartNoNewRun ensures an event that already
// produced a run is rejected even after the process restarts with a fresh
// engine over the same store.
func TestDuplicateEventAfterRestartNoNewRun(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := triggerCfg(dir, model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}})
	ev := pushEvent("restart-sha")

	eng := New(Options{Store: st, WorkDir: dir})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	id1, created1, err := eng.TriggerEvent(context.Background(), cfg, ev)
	if err != nil || !created1 {
		t.Fatalf("first trigger: created=%v err=%v", created1, err)
	}
	waitTerminal(t, eng, id1, 5*time.Second)
	stopEngine(t, eng)

	// Restart: brand-new engine over the same store.
	eng2 := New(Options{Store: st, WorkDir: dir})
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopEngine(t, eng2) })

	id2, created2, err := eng2.TriggerEvent(context.Background(), cfg, ev)
	if err != nil {
		t.Fatal(err)
	}
	if created2 || id2 != "" {
		t.Fatalf("duplicate event after restart created run %q", id2)
	}
	runs, _ := eng2.GetRuns(context.Background())
	if len(runs) != 1 {
		t.Fatalf("run count after restart = %d, want 1", len(runs))
	}
}

func stopEngine(t *testing.T, eng *Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("stop engine: %v", err)
	}
}

// ---- cancellation ----------------------------------------------------------

// TestCancelImmediatelyAfterCreateAlwaysConverges hammers the cancel/start
// race: a run is canceled the instant it is created, before its worker
// goroutine necessarily started. Every iteration must converge to canceled
// in bounded time, never stay running and never leak a child process.
func TestCancelImmediatelyAfterCreateAlwaysConverges(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Tasks: []model.Task{shellTask("s", "sleep 0.2")}},
		model.Stage{Name: "b", Needs: []string{"a"}, Tasks: []model.Task{shellTask("s", "sleep 0.2")}},
	)
	for i := 0; i < 30; i++ {
		id, created, err := eng.TriggerEvent(context.Background(), cfg, pushEvent(fmt.Sprintf("cancel-imm-%d", i)))
		if err != nil || !created {
			t.Fatalf("iter %d trigger: created=%v err=%v", i, created, err)
		}
		// Cancel immediately, racing the run's startup.
		if err := eng.CancelRun(context.Background(), id, "immediate cancel"); err != nil {
			t.Fatalf("iter %d cancel: %v", i, err)
		}
		run := waitTerminal(t, eng, id, 5*time.Second)
		if run.Status != model.StatusCanceled {
			t.Fatalf("iter %d: status = %s, want canceled", i, run.Status)
		}
		for _, st := range run.Stages {
			if st.Status == model.StatusRunning {
				t.Fatalf("iter %d: stage %s left running", i, st.Name)
			}
		}
	}
	waitNoChildren(t, eng, 5*time.Second)
}

// TestCancelDuringApprovalLeavesNoChildren cancels a run parked at an
// approval gate. The cancel must take effect immediately, the approval
// record must keep the cancel reason, the downstream stage must not start
// and no child process may be left behind.
func TestCancelDuringApprovalLeavesNoChildren(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	marker := filepath.Join(dir, "downstream-ran")
	cfg := triggerCfg(dir,
		model.Stage{
			Name:     "gate",
			Approval: &model.ApprovalRule{Required: true},
			Tasks:    []model.Task{shellTask("ok", "true")},
		},
		model.Stage{
			Name: "down", Needs: []string{"gate"},
			Tasks: []model.Task{shellTask("s", "touch "+marker+"; sleep 30")},
		},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("cancel-approval"))
	waitApprovalPending(t, eng, id, "gate", 3*time.Second)

	if err := eng.CancelRun(context.Background(), id, "abort while waiting"); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusCanceled {
		t.Fatalf("status = %s, want canceled", run.Status)
	}
	// The approval record keeps the cancel reason.
	gate := run.Stage("gate")
	if gate.Approval == nil || gate.Approval.Status != model.StatusCanceled {
		t.Fatalf("approval = %+v, want canceled", gate.Approval)
	}
	if gate.Approval.Comment != "abort while waiting" {
		t.Fatalf("approval comment = %q, want the cancel reason", gate.Approval.Comment)
	}
	// The downstream stage never started.
	if down := run.Stage("down"); down.Status == model.StatusRunning || down.Status == model.StatusSuccess {
		t.Fatalf("downstream stage ran after cancel: %s", down.Status)
	}
	// No child process was left behind and the downstream task never ran.
	waitNoChildren(t, eng, 5*time.Second)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("downstream task executed after cancel")
	}
}

// TestMarkUnfinishedConvergesWhenStagesTerminal is a regression test for the
// case where every stage already reached a terminal state but the run record
// still says running: the cancellation must still move the run to canceled
// instead of leaving it stuck.
func TestMarkUnfinishedConvergesWhenStagesTerminal(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir, model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}})
	run := buildRunFixture(cfg, pushEvent("stuck"), stageState("a", model.StatusSuccess))
	run.Status = model.StatusRunning // all stages done, run not finalized
	if err := eng.st.SaveRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	eng.markUnfinished(context.Background(), run, model.StatusCanceled, model.StatusCanceled, "stuck run canceled")
	got, err := eng.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.StatusCanceled {
		t.Fatalf("status = %s, want canceled (run must converge)", got.Status)
	}
}
