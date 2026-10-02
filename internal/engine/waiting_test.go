package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

// TestWaitingStatusVisibleWhileAwaitingApproval verifies that a run blocked
// at an approval gate is reported as waiting (run + stage) rather than
// pending or running.
func TestWaitingStatusVisibleWhileAwaitingApproval(t *testing.T) {
	eng, dir := newTestEngine(t, 0)
	cfg := triggerCfg(dir,
		model.Stage{Name: "build", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{
			Name:     "deploy",
			Needs:    []string{"build"},
			Approval: &model.ApprovalRule{Required: true},
			Tasks:    []model.Task{shellTask("ok", "true")},
		},
		model.Stage{Name: "after", Needs: []string{"deploy"}, Tasks: []model.Task{shellTask("ok", "true")}},
	)
	id, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("wait-appr"))

	waitStageStatus(t, eng, id, "deploy", model.StatusWaiting, 3*time.Second)
	waitRunStatus(t, eng, id, model.StatusWaiting, 3*time.Second)

	// The stage first waits for its upstream (no approval record yet); once
	// the gate is reached the pending approval must be visible while the
	// run/stage stay waiting.
	deadline := time.Now().Add(3 * time.Second)
	var run *model.Run
	for time.Now().Before(deadline) {
		run, _ = eng.GetRun(context.Background(), id)
		if dep := run.Stage("deploy"); dep.Approval != nil && dep.Approval.Status == model.StatusPending {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	dep := run.Stage("deploy")
	if dep.Approval == nil || dep.Approval.Status != model.StatusPending {
		t.Fatalf("approval record missing/pending: %+v", dep.Approval)
	}
	if run.Status != model.StatusWaiting || dep.Status != model.StatusWaiting {
		t.Fatalf("run=%s stage=%s, both must stay waiting at the gate", run.Status, dep.Status)
	}
	if run.Stage("after").Status != model.StatusWaiting {
		t.Fatalf("stage blocked on upstream = %s, want waiting", run.Stage("after").Status)
	}

	// Cancel while waiting: takes effect immediately, records the reason on
	// the approval, starts no downstream stage, leaves no child processes.
	if err := eng.CancelRun(context.Background(), id, "abort during review"); err != nil {
		t.Fatal(err)
	}
	assertRunSettlesCanceled(t, eng, id)

	run, _ = eng.GetRun(context.Background(), id)
	dep = run.Stage("deploy")
	if dep.Approval == nil || dep.Approval.Status != model.StatusCanceled ||
		dep.Approval.Comment != "abort during review" {
		t.Fatalf("approval must retain cancel reason, got %+v", dep.Approval)
	}
	if run.Stage("after").Status != model.StatusCanceled {
		t.Fatalf("downstream after cancel = %s, want canceled", run.Stage("after").Status)
	}
}

// TestWaitingStatusSurvivesRestart simulates a crash while a stage is parked
// at its approval gate. After resume recovery the run and stage must still
// be reported as waiting and the gate must remain approvable.
func TestWaitingStatusSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	st, err := store.NewFileStore(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := triggerCfg(dir,
		model.Stage{Name: "build", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{
			Name:     "deploy",
			Needs:    []string{"build"},
			Approval: &model.ApprovalRule{Required: true},
			Tasks:    []model.Task{shellTask("ok", "true")},
		},
	)

	// Build the exact on-disk state a crash leaves behind: build succeeded
	// and deploy is parked at its approval gate (run + stage = waiting with
	// a persisted pending approval). This avoids any live-engine goroutine
	// continuing to mutate the file after the simulated crash.
	now := time.Now()
	deadline := now.Add(2 * time.Hour)
	fixture := &model.Run{
		ID:         "run-wait-crash",
		Pipeline:   cfg.Name,
		EventID:    pushEvent("wait-restart").ID,
		ConfigJSON: snapshotConfig(cfg),
		Branch:     "main",
		Commit:     "wait-restart",
		Status:     model.StatusWaiting,
		CreatedAt:  now,
		UpdatedAt:  now,
		Stages: []model.StageState{
			{
				Name: "build", Status: model.StatusSuccess,
				StartedAt: &now, FinishedAt: &now,
				Tasks: []model.TaskState{{Name: "ok", Status: model.StatusSuccess,
					StartedAt: &now, FinishedAt: &now}},
			},
			{
				Name: "deploy", Status: model.StatusWaiting,
				Approval: &model.ApprovalState{
					Required: true, Status: model.StatusPending,
					CreatedAt: now, Deadline: &deadline,
				},
				Tasks: []model.TaskState{{Name: "ok", Status: model.StatusPending}},
			},
		},
	}
	if err := st.SaveRun(context.Background(), fixture); err != nil {
		t.Fatal(err)
	}
	id := fixture.ID

	// Sanity: the persisted snapshot shows waiting on both levels.
	snapshot, err := st.LoadRun(context.Background(), id)
	if err != nil {
		t.Fatalf("load waiting snapshot: %v", err)
	}
	if snapshot.Status != model.StatusWaiting ||
		snapshot.Stage("deploy").Status != model.StatusWaiting ||
		snapshot.Stage("deploy").Approval == nil ||
		snapshot.Stage("deploy").Approval.Status != model.StatusPending {
		t.Fatalf("bad crash snapshot: run=%s deploy=%s approval=%+v",
			snapshot.Status, snapshot.Stage("deploy").Status, snapshot.Stage("deploy").Approval)
	}

	// New process: recovery must keep the waiting semantics.
	eng2 := New(Options{Store: st, WorkDir: dir})
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng2.Stop(ctx)
	})
	if _, err := eng2.Recover(context.Background(), RecoveryResume); err != nil {
		t.Fatal(err)
	}
	waitRunStatus(t, eng2, id, model.StatusWaiting, 3*time.Second)
	waitStageStatus(t, eng2, id, "deploy", model.StatusWaiting, 3*time.Second)

	// The gate is still live: approving after restart completes the run.
	if _, err := eng2.Approve(context.Background(), id, "deploy", "alice"); err != nil {
		t.Fatalf("approve after restart: %v", err)
	}
	run := waitTerminal(t, eng2, id, 5*time.Second)
	if run.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", run.Status, run.Reason)
	}
}

// TestDuplicateScansAndReplayedEventsCreateOneRun delivers the same event
// repeatedly (as repeated watcher scans or a process restart would) and
// asserts a single run is created.
func TestDuplicateScansAndReplayedEventsCreateOneRun(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	st, err := store.NewFileStore(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	eng := New(Options{Store: st, WorkDir: dir})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := triggerCfg(dir, model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}})
	ev := pushEvent("dup-scan-1")

	var firstID string
	for n := 0; n < 10; n++ {
		id, created, err := eng.TriggerEvent(context.Background(), cfg, ev)
		if err != nil {
			t.Fatalf("delivery %d: %v", n, err)
		}
		if n == 0 {
			if !created || id == "" {
				t.Fatalf("first delivery must create a run, created=%v", created)
			}
			firstID = id
			continue
		}
		if created || id != "" {
			t.Fatalf("delivery %d created a duplicate run %q", n, id)
		}
	}

	waitTerminal(t, eng, firstID, 5*time.Second)

	// Simulate a process restart against the same data directory: event
	// records on disk must still reject the replay.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := eng.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()

	st2, err := store.NewFileStore(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := New(Options{Store: st2, WorkDir: dir})
	if err := eng2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, f := context.WithTimeout(context.Background(), 5*time.Second)
		defer f()
		_ = eng2.Stop(c)
	})
	id, created, err := eng2.TriggerEvent(context.Background(), cfg, ev)
	if err != nil || created || id != "" {
		t.Fatalf("event replay after restart must be rejected: id=%q created=%v err=%v", id, created, err)
	}
	runs, _ := eng2.GetRuns(context.Background())
	if len(runs) != 1 {
		t.Fatalf("want 1 run after restart replay, got %d", len(runs))
	}
}

func waitStageStatusPersisted(t *testing.T, st interface {
	LoadRun(context.Context, string) (*model.Run, error)
}, id, stage string, want model.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		run, err := st.LoadRun(context.Background(), id)
		if err == nil {
			if s := run.Stage(stage); s != nil && s.Status == want {
				return
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("persisted stage %s did not reach %q within %v", stage, want, timeout)
}
