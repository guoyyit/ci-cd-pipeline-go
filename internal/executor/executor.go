// Package executor runs pipeline task commands as local child processes.
//
// Every task runs in its own process group so that canceling a task can kill
// the whole process tree (task + children such as shells) with no orphaned
// grandchildren. Active processes are tracked so the daemon can verify at
// shutdown that no subprocess survives.
package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// Result is the outcome of a task execution.
type Result struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

// TaskError describes a task that exited with a non-zero status.
type TaskError struct {
	ExitCode int
	Message  string
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("task exited with status %d: %s", e.ExitCode, e.Message)
}

// ErrCanceled indicates the task was killed because its context was canceled.
var ErrCanceled = errors.New("task canceled")

// Executor starts and supervises task subprocesses.
type Executor struct {
	mu     sync.Mutex
	active map[*exec.Cmd]struct{}
}

// NewExecutor constructs an Executor.
func NewExecutor() *Executor {
	return &Executor{active: make(map[*exec.Cmd]struct{})}
}

// ActiveCount returns the number of currently running child processes.
func (e *Executor) ActiveCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.active)
}

// ActiveCmds returns snapshots of currently tracked command paths.
func (e *Executor) ActiveCmds() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.active))
	for c := range e.active {
		out = append(out, strings.Join(append([]string{c.Path}, c.Args[1:]...), " "))
	}
	return out
}

func (e *Executor) track(c *exec.Cmd) func() {
	e.mu.Lock()
	e.active[c] = struct{}{}
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		delete(e.active, c)
		e.mu.Unlock()
	}
}

// RunTask executes one task, honoring ctx cancellation/deadline.
//
// Shell tasks run as "sh -c <command>"; others run Command with Args.
// Output is captured (bounded) and returned with the result. When ctx is
// canceled the whole process group is SIGKILLed and RunTask returns
// ErrCanceled; non-zero exits return *TaskError.
func (e *Executor) RunTask(ctx context.Context, task model.Task, workdir string) (Result, error) {
	name, args := task.Command, task.Args
	if task.Shell {
		name = "sh"
		args = []string{"-c", task.Command}
	}
	cmd := exec.Command(name, args...)
	cmd.Dir = workdir
	if task.Workdir != "" {
		cmd.Dir = resolveWorkdir(workdir, task.Workdir)
	}
	env, err := buildEnv(task.Env)
	if err != nil {
		return Result{}, err
	}
	cmd.Env = env

	var out, errOut cappedBuffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut

	if err := startWithProcessGroup(cmd); err != nil {
		return Result{}, fmt.Errorf("start task %q: %w", task.Name, err)
	}
	untrack := e.track(cmd)
	defer untrack()

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	var waitResult error
	select {
	case werr := <-waitErr:
		waitResult = werr
	case <-ctx.Done():
		// Propagate cancellation to the whole process group, then reap.
		_ = killProcessGroup(cmd)
		select {
		case <-waitErr:
		case <-time.After(5 * time.Second):
		}
		res := Result{Stdout: out.String(), Stderr: errOut.String()}
		return res, fmt.Errorf("%w: %w", ErrCanceled, ctx.Err())
	}

	res := Result{Stdout: out.String(), Stderr: errOut.String()}
	if waitResult != nil {
		var ee *exec.ExitError
		if errors.As(waitResult, &ee) {
			res.ExitCode = ee.ExitCode()
			return res, &TaskError{ExitCode: ee.ExitCode(), Message: strings.TrimSpace(errOut.String())}
		}
		return res, fmt.Errorf("run task %q: %w", task.Name, waitResult)
	}
	res.ExitCode = 0
	return res, nil
}

// WaitIdle blocks until no child processes remain or ctx expires.
func (e *Executor) WaitIdle(ctx context.Context) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if e.ActiveCount() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func resolveWorkdir(base, sub string) string {
	if strings.HasPrefix(sub, "/") {
		return sub
	}
	return base + "/" + sub
}

func buildEnv(extra []string) ([]string, error) {
	env := os.Environ()
	for _, kv := range extra {
		if !strings.Contains(kv, "=") {
			return nil, fmt.Errorf("invalid env entry %q: expected KEY=VALUE", kv)
		}
		env = append(env, kv)
	}
	return env, nil
}

const maxOutputBytes = 1 << 20 // 1 MiB per stream

type cappedBuffer struct {
	buf bytes.Buffer
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	remaining := maxOutputBytes - c.buf.Len()
	if remaining <= 0 {
		return len(p), nil // discard overflow but pretend success
	}
	if len(p) > remaining {
		c.buf.Write(p[:remaining])
		return len(p), nil
	}
	return c.buf.Write(p)
}

func (c *cappedBuffer) String() string { return c.buf.String() }

var _ io.Writer = (*cappedBuffer)(nil)
