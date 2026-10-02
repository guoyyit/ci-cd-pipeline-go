package engine

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// assertRunSettlesCanceled polls until the run is canceled (or fails the
// test) and independently verifies that no child process survives.
func assertRunSettlesCanceled(t *testing.T, eng *Engine, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var run *model.Run
	for time.Now().Before(deadline) {
		run, _ = eng.GetRun(context.Background(), id)
		if run != nil && run.Status.IsTerminal() {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if run == nil || !run.Status.IsTerminal() {
		t.Fatalf("run %s never settled: %+v", id, run)
	}
	if run.Status != model.StatusCanceled {
		t.Fatalf("run %s status = %s, want canceled (%s)", id, run.Status, run.Reason)
	}
	idleDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(idleDeadline) && eng.ActiveChildren() != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if n := eng.ActiveChildren(); n != 0 {
		t.Fatalf("%d child process(es) survived cancellation of %s", n, id)
	}
}

// TestCancelImmediatelyAfterTrigger issues the cancel right after
// TriggerEvent returns. The run must settle to canceled in bounded time,
// must never start its task, and must leave no child process behind.
func TestCancelImmediatelyAfterTrigger(t *testing.T) {
	const iterations = 60
	for i := 0; i < iterations; i++ {
		eng, dir := newTestEngine(t, 0)
		marker := filepath.Join(dir, "started-"+strconv.Itoa(i))
		cfg := triggerCfg(dir,
			model.Stage{Name: "s", Tasks: []model.Task{
				shellTask("work", fmt.Sprintf("touch %s; sleep 30", marker)),
			}},
		)
		ev := pushEvent("race-immediate-" + strconv.Itoa(i))

		id, created, err := eng.TriggerEvent(context.Background(), cfg, ev)
		if err != nil || !created {
			t.Fatalf("iter %d: trigger created=%v err=%v", i, created, err)
		}
		if err := eng.CancelRun(context.Background(), id, "immediate abort "+strconv.Itoa(i)); err != nil {
			t.Fatalf("iter %d: cancel: %v", i, err)
		}
		assertRunSettlesCanceled(t, eng, id)
		if out, statErr := exec.Command("test", "-f", marker).CombinedOutput(); statErr == nil {
			t.Fatalf("iter %d: canceled run started its task (marker %s exists) %s", i, marker, out)
		}
		stopEngine(t, eng)
	}
}

// TestCancelConcurrentWithTrigger races run creation against cancellation
// from separate goroutines. Regardless of ordering the result is
// deterministic: exactly one run exists and it is canceled.
func TestCancelConcurrentWithTrigger(t *testing.T) {
	const iterations = 60
	for i := 0; i < iterations; i++ {
		eng, dir := newTestEngine(t, 0)
		cfg := triggerCfg(dir,
			model.Stage{Name: "s", Tasks: []model.Task{shellTask("work", "sleep 30")}},
		)
		ev := pushEvent("race-concurrent-" + strconv.Itoa(i))

		var (
			id    string
			idMu  sync.Mutex
			idSet = make(chan struct{})
		)
		setID := func(v string) {
			idMu.Lock()
			if id == "" {
				id = v
				close(idSet)
			}
			idMu.Unlock()
		}
		getID := func() string {
			idMu.Lock()
			defer idMu.Unlock()
			return id
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			rid, _, _ := eng.TriggerEvent(context.Background(), cfg, ev)
			setID(rid)
		}()
		go func() {
			defer wg.Done()
			<-start
			select {
			case <-idSet:
			case <-time.After(2 * time.Second):
				return
			}
			_ = eng.CancelRun(context.Background(), getID(), "concurrent abort")
		}()
		close(start)
		wg.Wait()

		if getID() == "" {
			t.Fatalf("iter %d: run was not created", i)
		}
		assertRunSettlesCanceled(t, eng, getID())
		runs, _ := eng.GetRuns(context.Background())
		if len(runs) != 1 {
			t.Fatalf("iter %d: want exactly 1 run, got %d", i, len(runs))
		}
		stopEngine(t, eng)
	}
}

func stopEngine(t *testing.T, eng *Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.Stop(ctx); err != nil {
		t.Fatalf("engine stop: %v", err)
	}
}

// TestCancelWhileQueuedForSlot cancels a run that has not yet acquired a
// concurrency slot. It must settle to canceled without executing anything
// once the slot frees.
func TestCancelWhileQueuedForSlot(t *testing.T) {
	eng, dir := newTestEngine(t, 1)
	cfg := triggerCfg(dir,
		model.Stage{Name: "s", Tasks: []model.Task{shellTask("sleep", "sleep 1")}},
	)
	id1, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("q1"))
	id2, _, _ := eng.TriggerEvent(context.Background(), cfg, pushEvent("q2"))

	waitRunStatus(t, eng, id2, model.StatusPending, 2*time.Second)
	if err := eng.CancelRun(context.Background(), id2, "canceled while queued"); err != nil {
		t.Fatal(err)
	}
	assertRunSettlesCanceled(t, eng, id2)

	run, _ := eng.GetRun(context.Background(), id2)
	if run.Reason != "canceled while queued" {
		t.Fatalf("cancel reason not persisted: %q", run.Reason)
	}
	for _, st := range run.Stages {
		if st.Status != model.StatusCanceled {
			t.Fatalf("queued run stage %s = %s, want canceled", st.Name, st.Status)
		}
	}
	// The first run completes normally and frees the slot; the canceled run
	// must not have started when that happens.
	waitTerminal(t, eng, id1, 10*time.Second)
}
