package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

// ---- helpers ---------------------------------------------------------------

func newTestEngine(t *testing.T, concurrency int) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	eng := New(Options{Store: st, WorkDir: dir, Concurrency: concurrency, ArtifactRoot: dir})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	return eng, dir
}

func shellTask(name, command string) model.Task {
	return model.Task{Name: name, Shell: true, Command: command}
}

func triggerCfg(repo string, stages ...model.Stage) *model.Config {
	return &model.Config{
		Name:        "p",
		Repo:        repo,
		Triggers:    model.Triggers{Branches: []string{"main"}},
		Concurrency: 0,
		Stages:      stages,
	}
}

func pushEvent(sha string) model.Event {
	return model.Event{
		ID: "push:main:" + sha, Kind: model.KindPush, Repo: "/repo",
		Branch: "main", Commit: sha,
	}
}

func waitTerminal(t *testing.T, eng *Engine, id string, timeout time.Duration) *model.Run {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := eng.GetRun(context.Background(), id)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if run.Status.IsTerminal() {
			return run
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s did not finish within %v", id, timeout)
	return nil
}

func waitStageStatus(t *testing.T, eng *Engine, id, stage string, want model.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, _ := eng.GetRun(context.Background(), id)
		if st := run.Stage(stage); st != nil && st.Status == want {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("stage %s did not reach %q within %v", stage, want, timeout)
}

func stageByName(run *model.Run, name string) *model.StageState {
	return run.Stage(name)
}

// ---- scenarios -------------------------------------------------------------

func TestDuplicateEventCreatesOneRun(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir, model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}})
	ev := pushEvent("aaa111")

	id1, created1, err := eng.TriggerEvent(context.Background(), cfg, ev)
	if err != nil || !created1 {
		t.Fatalf("first trigger: created=%v err=%v", created1, err)
	}
	id2, created2, err := eng.TriggerEvent(context.Background(), cfg, ev)
	if err != nil || created2 || id2 != "" {
		t.Fatalf("duplicate trigger: created=%v id=%q err=%v", created2, id2, err)
	}
	run := waitTerminal(t, eng, id1, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", run.Status, run.Reason)
	}
	runs, _ := eng.GetRuns(context.Background())
	nonCanceled := 0
	for _, r := range runs {
		if r.Status != model.StatusCanceled {
			nonCanceled++
		}
	}
	if nonCanceled != 1 {
		t.Fatalf("want exactly 1 effective run, got %d", len(runs))
	}
}

func TestSerialDependencyFailureSkipsDownstream(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{Name: "b", Needs: []string{"a"}, Tasks: []model.Task{shellTask("boom", "exit 1")}},
		model.Stage{Name: "c", Needs: []string{"b"}, Tasks: []model.Task{shellTask("ok", "true")}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("s1"))
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusFailed {
		t.Fatalf("run status = %s, want failed", run.Status)
	}
	if stageByName(run, "a").Status != model.StatusSuccess ||
		stageByName(run, "b").Status != model.StatusFailed ||
		stageByName(run, "c").Status != model.StatusSkipped {
		t.Fatalf("unexpected stage states: a=%s b=%s c=%s",
			stageByName(run, "a").Status, stageByName(run, "b").Status, stageByName(run, "c").Status)
	}
}

func TestParallelGroupStagesBothRun(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	m1 := filepath.Join(dir, "p1")
	m2 := filepath.Join(dir, "p2")
	cfg := triggerCfg(dir,
		model.Stage{Name: "p1", Group: "g", Tasks: []model.Task{shellTask("t", "touch "+m1)}},
		model.Stage{Name: "p2", Group: "g", Tasks: []model.Task{shellTask("t", "touch "+m2)}},
		model.Stage{Name: "end", Tasks: []model.Task{shellTask("ok", "true")}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("par"))
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", run.Status, run.Reason)
	}
	for _, m := range []string{m1, m2} {
		if _, err := os.Stat(m); err != nil {
			t.Fatalf("parallel marker %s missing: %v", m, err)
		}
	}
}

func TestApprovalPassThenRunSucceeds(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{
			Name: "deploy", Needs: []string{"a"},
			Approval: &model.ApprovalRule{Required: true, Approvers: []string{"alice"}},
			Tasks:    []model.Task{shellTask("ok", "true")},
		},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("appr1"))
	waitStageStatus(t, eng, id, "deploy", model.StatusPending, 3*time.Second)

	// Unauthorized approver is rejected.
	if _, err := eng.Approve(context.Background(), id, "deploy", "mallory"); err == nil {
		t.Fatal("unexpected approver should be rejected")
	}
	if _, err := eng.Approve(context.Background(), id, "deploy", "alice"); err != nil {
		t.Fatalf("Approve: %v", err)
	}
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", run.Status, run.Reason)
	}
	dep := stageByName(run, "deploy")
	if dep.Approval == nil || dep.Approval.Status != model.StatusSuccess ||
		dep.Approval.Approver != "alice" || dep.Approval.DecidedAt == nil {
		t.Fatalf("approval not recorded: %+v", dep.Approval)
	}
}

func TestApprovalRejectFailsWithAuditComment(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{
			Name: "deploy", Needs: []string{"a"},
			Approval: &model.ApprovalRule{Required: true},
			Tasks:    []model.Task{shellTask("ok", "true")},
		},
		model.Stage{Name: "after", Needs: []string{"deploy"}, Tasks: []model.Task{shellTask("ok", "true")}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("appr2"))
	waitStageStatus(t, eng, id, "deploy", model.StatusPending, 3*time.Second)

	if _, err := eng.Reject(context.Background(), id, "deploy", "alice", "  "); err == nil {
		t.Fatal("reject without comment must fail")
	}
	if _, err := eng.Reject(context.Background(), id, "deploy", "alice", "missing sign-off"); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}
	dep := stageByName(run, "deploy")
	if dep.Status != model.StatusFailed ||
		dep.Approval == nil || dep.Approval.Status != model.StatusFailed ||
		dep.Approval.Comment != "missing sign-off" || dep.Approval.Approver != "alice" {
		t.Fatalf("rejection not auditable: %+v", dep.Approval)
	}
	if stageByName(run, "after").Status != model.StatusSkipped {
		t.Fatal("downstream of rejected stage must be skipped")
	}
}

func TestApprovalTimeoutCancels(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{
			Name:     "deploy",
			Approval: &model.ApprovalRule{Required: true, Timeout: model.Duration(100 * time.Millisecond)},
			Tasks:    []model.Task{shellTask("ok", "true")},
		},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("appr3"))
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusCanceled {
		t.Fatalf("status = %s, want canceled", run.Status)
	}
	dep := stageByName(run, "deploy")
	if dep.Approval == nil || dep.Approval.Status != model.StatusCanceled {
		t.Fatalf("approval state = %+v, want canceled", dep.Approval)
	}
}

func TestRetryEventuallySucceeds(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	flag := filepath.Join(dir, "retryflag")
	cmd := fmt.Sprintf(`if [ ! -f %s ]; then touch %s; exit 1; fi`, flag, flag)
	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Retry: 2, Tasks: []model.Task{shellTask("flaky", cmd)}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("retry1"))
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", run.Status, run.Reason)
	}
	if got := stageByName(run, "a").Attempts; got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestRetryExhaustedFails(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Retry: 1, Tasks: []model.Task{shellTask("boom", "exit 1")}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("retry2"))
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusFailed {
		t.Fatalf("status = %s", run.Status)
	}
	if got := stageByName(run, "a").Attempts; got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestRerunOnlyFailedStagesAndForce(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	counterA := filepath.Join(dir, "countA")
	counterB := filepath.Join(dir, "countB")
	failMarker := filepath.Join(dir, "failB")
	if err := os.WriteFile(failMarker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	incr := func(path string) string { return fmt.Sprintf(`echo x >> %s`, path) }
	failUntilRemoved := fmt.Sprintf(`if [ -f %s ]; then exit 1; fi`, failMarker)

	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Tasks: []model.Task{shellTask("count", incr(counterA))}},
		model.Stage{Name: "b", Needs: []string{"a"}, Tasks: []model.Task{
			shellTask("count", incr(counterB)),
			shellTask("maybe", failUntilRemoved),
		}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("rr1"))
	run := waitTerminal(t, eng, id, 5*time.Second)
	if stageByName(run, "a").Status != model.StatusSuccess || stageByName(run, "b").Status != model.StatusFailed {
		t.Fatalf("first run: a=%s b=%s", stageByName(run, "a").Status, stageByName(run, "b").Status)
	}

	if err := os.Remove(failMarker); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.RerunRun(context.Background(), id, false); err != nil {
		t.Fatalf("RerunRun: %v", err)
	}
	run = waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("rerun status = %s (%s)", run.Status, run.Reason)
	}
	// Successful stage a must not have been executed again.
	if n := countLines(t, counterA); n != 1 {
		t.Fatalf("stage a executed %d times, want 1 (successful stage reused)", n)
	}
	if n := countLines(t, counterB); n != 2 {
		t.Fatalf("stage b executed %d times, want 2", n)
	}

	// Forced rerun re-executes everything, including successful stages.
	if _, err := eng.RerunRun(context.Background(), id, true); err != nil {
		t.Fatalf("RerunRun force: %v", err)
	}
	run = waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("forced rerun status = %s", run.Status)
	}
	if n := countLines(t, counterA); n != 2 {
		t.Fatalf("stage a after force = %d, want 2", n)
	}
	if n := countLines(t, counterB); n != 3 {
		t.Fatalf("stage b after force = %d, want 3", n)
	}
}

func TestCancelRunKillsChildren(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "long", Tasks: []model.Task{shellTask("sleep", "sleep 30")}},
		model.Stage{Name: "down", Tasks: []model.Task{shellTask("ok", "true")}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("cancel1"))
	waitStageStatus(t, eng, id, "long", model.StatusRunning, 3*time.Second)

	done := make(chan error, 1)
	go func() { done <- eng.CancelRun(context.Background(), id, "user abort") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CancelRun: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CancelRun blocked")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, _ := eng.GetRun(context.Background(), id)
		if run.Status.IsTerminal() {
			if run.Status != model.StatusCanceled {
				t.Fatalf("status = %s, want canceled", run.Status)
			}
			if stageByName(run, "down").Status != model.StatusSkipped &&
				stageByName(run, "down").Status != model.StatusCanceled {
				t.Fatalf("downstream stage = %s", stageByName(run, "down").Status)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if eng.ActiveChildren() != 0 {
		t.Fatalf("%d child processes still running after cancel", eng.ActiveChildren())
	}
}

func TestConcurrencyLimitQueuesRuns(t *testing.T) {
	eng, dir := newTestEngine(t, 1)
	cfg := triggerCfg(dir,
		model.Stage{Name: "long", Tasks: []model.Task{shellTask("sleep", "sleep 1")}},
	)
	id1, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("c1"))
	id2, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("c2"))

	// Within the run, at least one poll must show one running and one pending.
	sawQueued := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r1, _ := eng.GetRun(context.Background(), id1)
		r2, _ := eng.GetRun(context.Background(), id2)
		states := map[string]int{}
		states[string(r1.Status)]++
		states[string(r2.Status)]++
		if states["running"] == 1 && states["pending"] == 1 {
			sawQueued = true
			break
		}
		if r1.Status.IsTerminal() && r2.Status.IsTerminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawQueued {
		t.Fatal("expected one run queued (pending) while the other was running")
	}
	waitTerminal(t, eng, id1, 10*time.Second)
	waitTerminal(t, eng, id2, 10*time.Second)
}

func TestRunTimeoutCancels(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir)
	cfg.RunTimeout = model.Duration(150 * time.Millisecond)
	cfg.Stages = []model.Stage{{Name: "long", Tasks: []model.Task{shellTask("sleep", "sleep 30")}}}
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("to1"))
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusCanceled {
		t.Fatalf("status = %s, want canceled", run.Status)
	}
	if eng.ActiveChildren() != 0 {
		t.Fatal("timeout left a child process running")
	}
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n := 0
	for _, b := range data {
		if b == '\n' {
			n++
		}
	}
	return n
}

func TestArtifactsCollectedAndPersisted(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	task := model.Task{
		Name: "gen", Shell: true,
		Command:   "mkdir -p out && echo report > out/report.txt",
		Artifacts: []string{"out/*.txt"},
	}
	cfg := triggerCfg(dir, model.Stage{Name: "a", Tasks: []model.Task{task}})
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("art1"))
	run := waitTerminal(t, eng, id, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", run.Status, run.Reason)
	}
	ts := stageByName(run, "a").Tasks[0]
	if len(ts.Artifacts) != 1 || ts.Artifacts[0] != filepath.ToSlash(filepath.Join("out", "report.txt")) {
		t.Fatalf("artifacts recorded = %v", ts.Artifacts)
	}
	collected := filepath.Join(dir, "artifacts", id, "a", "gen", "out", "report.txt")
	data, err := os.ReadFile(collected)
	if err != nil {
		t.Fatalf("artifact not collected at %s: %v", collected, err)
	}
	if string(data) != "report\n" {
		t.Fatalf("artifact content = %q", string(data))
	}
}
