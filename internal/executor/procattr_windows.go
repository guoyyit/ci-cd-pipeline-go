//go:build windows

package executor

import "os/exec"

// On Windows there is no process-group signal equivalent; kill the process.
// Child processes spawned by a shell may survive, which is documented.
func startWithProcessGroup(cmd *exec.Cmd) error { return cmd.Start() }

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
