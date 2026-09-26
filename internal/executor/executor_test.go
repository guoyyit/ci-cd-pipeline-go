package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

func TestRunTaskSuccessAndOutput(t *testing.T) {
	e := NewExecutor()
	res, err := e.RunTask(context.Background(),
		model.Task{Name: "echo", Shell: true, Command: "echo hello && echo err 1>&2"}, t.TempDir())
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d", res.ExitCode)
	}
	if res.Stdout != "hello\n" {
		t.Fatalf("stdout = %q", res.Stdout)
	}
	if res.Stderr != "err\n" {
		t.Fatalf("stderr = %q", res.Stderr)
	}
}

func TestRunTaskFailureExitCode(t *testing.T) {
	e := NewExecutor()
	_, err := e.RunTask(context.Background(),
		model.Task{Name: "fail", Shell: true, Command: "exit 7"}, t.TempDir())
	var te *TaskError
	if !errors.As(err, &te) || te.ExitCode != 7 {
		t.Fatalf("want TaskError exit 7, got %v", err)
	}
}

// TestCancelKillsProcessTree starts a shell spawning a long-lived grandchild,
// cancels the context and verifies the whole group is reaped with no tracked
// children left behind.
func TestCancelKillsProcessTree(t *testing.T) {
	e := NewExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := e.RunTask(ctx,
			// sh starts a child sleep that keeps the group alive.
			model.Task{Name: "long", Shell: true, Command: "sleep 30 & sleep 30"}, t.TempDir())
		done <- err
	}()

	// Wait until the process is really running.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && e.ActiveCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if e.ActiveCount() == 0 {
		t.Fatal("task never became active")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("want ErrCanceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RunTask did not return after cancel")
	}

	idleCtx, c := context.WithTimeout(context.Background(), time.Second)
	defer c()
	if err := e.WaitIdle(idleCtx); err != nil {
		t.Fatalf("children still tracked after cancel: %d (%v)", e.ActiveCount(), err)
	}
}

func TestTimeoutCancelsTask(t *testing.T) {
	e := NewExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := e.RunTask(ctx, model.Task{Name: "slow", Shell: true, Command: "sleep 5"}, t.TempDir())
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("want ErrCanceled, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("task was not killed when deadline expired")
	}
}
