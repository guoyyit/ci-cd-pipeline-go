//go:build !windows

package executor

import (
	"os/exec"
	"syscall"
)

// startWithProcessGroup starts cmd in a new process group so the whole tree
// can be signaled together.
func startWithProcessGroup(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	return cmd.Start()
}

// killProcessGroup sends SIGKILL to the entire group (negative pid).
// Falls back to killing the single process when group signaling fails.
func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	pgid := cmd.Process.Pid
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
		return cmd.Process.Kill()
	}
	return nil
}
