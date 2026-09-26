package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

// TestRecoveryResumeInterruptedRun simulates a crash: stage "a" succeeded and
// stage "b" was left in "running" on disk. A new engine must keep "a",
// re-execute "b" (and its downstream) and finish successfully.
func TestRecoveryResumeInterruptedRun(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}

	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{Name: "b", Needs: []string{"a"}, Tasks: []model.Task{shellTask("ok", "true")}},
	)
	run := buildRunFixture(cfg, pushEvent("crash1"),
		stageState("a", model.StatusSuccess),
		stageState("b", model.StatusRunning),
	)
	run.Status = model.StatusRunning
	if err := st.SaveRun(context.Background(), run); err != nil {
		t.Fatal(err)
	}

	// Brand-new engine = process restart.
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
	got := waitTerminal(t, eng, run.ID, 5*time.Second)
	if got.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", got.Status, got.Reason)
	}
	if got.Stage("a").Status != model.StatusSuccess {
		t.Fatal("successful stage must be preserved after resume")
	}
	if got.Stage("b").Attempts < 1 {
		t.Fatal("interrupted stage should have been re-executed")
	}
}

// TestRecoveryResumeSkipsAlreadySucceededDownstreamNotRequiredButReRunsTainted
// covers the rule that a successful stage downstream of an interrupted stage
// cannot be trusted (it logically ran before the interruption) only when it
// depends on the interrupted stage; unrelated success stays.
func TestRecoveryResumeTaintsDownstream(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(filepath.Join(dir, "data"))

	cfg := triggerCfg(dir,
		model.Stage{Name: "a", Group: "g", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{Name: "b", Group: "g", Tasks: []model.Task{shellTask("ok", "true")}},
		model.Stage{Name: "c", Needs: []string{"b"}, Tasks: []model.Task{shellTask("ok", "true")}},
	)
	run := buildRunFixture(cfg, pushEvent("crash2"),
		stageState("a", model.StatusSuccess),
		stageState("b", model.StatusRunning),
		stageState("c", model.StatusSuccess), // cannot trust: depends on b
	)
	run.Status = model.StatusRunning
	_ = st.SaveRun(context.Background(), run)

	eng := New(Options{Store: st, WorkDir: dir})
	_ = eng.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	if _, err := eng.Recover(context.Background(), RecoveryResume); err != nil {
		t.Fatal(err)
	}
	got := waitTerminal(t, eng, run.ID, 5*time.Second)
	if got.Status != model.StatusSuccess {
		t.Fatalf("status = %s (%s)", got.Status, got.Reason)
	}
	if got.Stage("c").Status != model.StatusSuccess {
		t.Fatal("tainted downstream c should re-run to success")
	}
	if n := got.Stage("c").Attempts; n < 1 {
		t.Fatal("c should have been re-executed after resume")
	}
}

func TestRecoveryCancelAndFailPolicies(t *testing.T) {
	for _, policy := range []RecoveryPolicy{RecoveryCancel, RecoveryFail} {
		t.Run(string(policy), func(t *testing.T) {
			dir := t.TempDir()
			st, _ := store.NewFileStore(filepath.Join(dir, "data"))
			cfg := triggerCfg(dir,
				model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}},
			)
			run := buildRunFixture(cfg, pushEvent("crash3"), stageState("a", model.StatusRunning))
			run.Status = model.StatusRunning
			_ = st.SaveRun(context.Background(), run)

			eng := New(Options{Store: st, WorkDir: dir})
			_ = eng.Start(context.Background())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = eng.Stop(ctx)
			})
			if _, err := eng.Recover(context.Background(), policy); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			var got *model.Run
			for time.Now().Before(deadline) {
				got, _ = eng.GetRun(context.Background(), run.ID)
				if got.Status.IsTerminal() {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			want := model.StatusCanceled
			if policy == RecoveryFail {
				want = model.StatusFailed
			}
			if got.Status != want {
				t.Fatalf("status = %s, want %s", got.Status, want)
			}
		})
	}
}

// TestRecoveryDoesNotReexecuteFinishedRuns ensures terminal runs are left
// alone at startup (no duplicate execution after restart).
func TestRecoveryIgnoresTerminalRuns(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(filepath.Join(dir, "data"))
	cfg := triggerCfg(dir, model.Stage{Name: "a", Tasks: []model.Task{shellTask("ok", "true")}})
	run := buildRunFixture(cfg, pushEvent("done"), stageState("a", model.StatusSuccess))
	now := time.Now()
	run.Status = model.StatusSuccess
	run.FinishedAt = &now
	_ = st.SaveRun(context.Background(), run)

	eng := New(Options{Store: st, WorkDir: dir})
	_ = eng.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	ids, err := eng.Recover(context.Background(), RecoveryResume)
	if err != nil || len(ids) != 0 {
		t.Fatalf("terminal run must not be recovered, got %v %v", ids, err)
	}
}

// ---- small builders --------------------------------------------------------

func stageState(name string, status model.Status) model.StageState {
	ss := model.StageState{Name: name, Status: status}
	ss.Tasks = append(ss.Tasks, model.TaskState{Name: "ok", Status: status})
	return ss
}

// buildRunFixture constructs an on-disk run with the given stage states,
// simulating a run left behind by a crashed process.
func buildRunFixture(cfg *model.Config, ev model.Event, stages ...model.StageState) *model.Run {
	now := time.Now()
	return &model.Run{
		ID:         fmt.Sprintf("run-%d", now.UnixNano()),
		Pipeline:   cfg.Name,
		EventID:    ev.ID,
		ConfigJSON: snapshotConfig(cfg),
		Branch:     ev.Branch,
		Commit:     ev.Commit,
		Status:     model.StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
		Stages:     stages,
	}
}
