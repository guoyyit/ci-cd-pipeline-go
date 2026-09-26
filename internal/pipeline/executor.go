package pipeline

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Executor 在本机执行任务命令。
type Executor struct {
	WorkDir      string // 任务工作目录（通常为仓库检出目录）
	ArtifactRoot string // 产物与日志根目录
}

// NewExecutor 创建执行器。
func NewExecutor(workDir, artifactRoot string) *Executor {
	return &Executor{WorkDir: workDir, ArtifactRoot: artifactRoot}
}

// TaskResult 是一次任务执行的结果。
type TaskResult struct {
	ExitCode int
	LogPath  string
	Err      error
}

// RunTask 执行单个任务：
//   - 使用独立进程组，取消时向整个进程组发 SIGKILL，不遗留子进程；
//   - 支持任务级超时；
//   - 输出写入日志文件，结束后收集声明的产物。
func (e *Executor) RunTask(ctx context.Context, runID, stageName string, task TaskConfig) TaskResult {
	if task.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, task.Timeout.Duration)
		defer cancel()
	}

	logDir := filepath.Join(e.ArtifactRoot, "runs", runID, "logs", stageName)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return TaskResult{ExitCode: -1, Err: fmt.Errorf("create log dir: %w", err)}
	}
	logPath := filepath.Join(logDir, sanitize(task.Name)+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return TaskResult{ExitCode: -1, Err: fmt.Errorf("create log file: %w", err)}
	}
	defer logFile.Close()

	fmt.Fprintf(logFile, "$ %s\n\n", task.Run)

	cmd := exec.CommandContext(ctx, "sh", "-c", task.Run)
	cmd.Dir = e.WorkDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = mergeEnv(os.Environ(), task.Env)
	// 独立进程组，便于整组终止。
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// 取消时杀死整个进程组而不是仅父进程。
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 5 * time.Second

	runErr := cmd.Run()
	exitCode := 0
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	} else if runErr != nil {
		exitCode = -1
	}

	res := TaskResult{ExitCode: exitCode, LogPath: logPath, Err: runErr}

	if ctx.Err() != nil {
		fmt.Fprintf(logFile, "\n[task terminated: %v]\n", ctx.Err())
		res.Err = ctx.Err()
		return res
	}

	// 收集产物（相对 WorkDir 的 glob）。
	if len(task.Artifacts) > 0 {
		artDir := filepath.Join(e.ArtifactRoot, "runs", runID, "artifacts", stageName, sanitize(task.Name))
		if err := collectArtifacts(e.WorkDir, artDir, task.Artifacts); err != nil {
			fmt.Fprintf(logFile, "\n[artifact collection error: %v]\n", err)
		}
	}
	return res
}

// mergeEnv 合并环境变量，后者覆盖前者。
func mergeEnv(base []string, extra map[string]string) []string {
	if len(extra) == 0 {
		return base
	}
	out := make([]string, 0, len(base)+len(extra))
	skip := map[string]bool{}
	for k := range extra {
		skip[k] = true
	}
	for _, kv := range base {
		if i := strings.IndexByte(kv, '='); i > 0 && skip[kv[:i]] {
			continue
		}
		out = append(out, kv)
	}
	for k, v := range extra {
		out = append(out, k+"="+v)
	}
	return out
}

// collectArtifacts 按 glob 规则复制产物文件。
func collectArtifacts(workDir, destDir string, patterns []string) error {
	for _, p := range patterns {
		matches, err := filepath.Glob(filepath.Join(workDir, p))
		if err != nil {
			return fmt.Errorf("bad artifact pattern %q: %w", p, err)
		}
		for _, m := range matches {
			info, err := os.Stat(m)
			if err != nil || info.IsDir() {
				continue
			}
			rel, err := filepath.Rel(workDir, m)
			if err != nil {
				rel = filepath.Base(m)
			}
			dest := filepath.Join(destDir, rel)
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				return err
			}
			if err := copyFile(m, dest); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

// sanitize 将名称转为安全文件名。
func sanitize(name string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", " ", "_", "..", "_")
	return r.Replace(name)
}
