// Package git reads commit and tag events from local on-disk Git repositories
// by shelling out to the system "git" binary. No network access is performed.
package git

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// Ref is a branch head or tag observed in a repository.
type Ref struct {
	Kind   model.Kind
	Name   string // branch or tag name without refs/ prefix
	Commit string // peeled commit SHA
}

// run executes a git command in repoPath and returns trimmed stdout.
func run(ctx context.Context, repoPath string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repoPath}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// IsRepo reports whether path is a Git working tree.
func IsRepo(ctx context.Context, path string) bool {
	out, err := run(ctx, path, "rev-parse", "--is-inside-work-tree")
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) == "true"
}

// ScanRefs lists local branch heads and tags (lightweight and annotated).
// Output lines are "<sha> <refname>"; tags are peeled to the target commit.
func ScanRefs(ctx context.Context, repoPath string) ([]Ref, error) {
	out, err := run(ctx, repoPath, "for-each-ref",
		"--format=%(objectname) %(refname)", "refs/heads", "refs/tags")
	if err != nil {
		return nil, err
	}
	var refs []Ref
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		sha, refname := fields[0], fields[1]
		switch {
		case strings.HasPrefix(refname, "refs/heads/"):
			refs = append(refs, Ref{Kind: model.KindPush, Name: strings.TrimPrefix(refname, "refs/heads/"), Commit: sha})
		case strings.HasPrefix(refname, "refs/tags/"):
			name := strings.TrimPrefix(refname, "refs/tags/")
			// Peel annotated tags (^{}) to the underlying commit.
			peeled, perr := run(ctx, repoPath, "rev-list", "-n", "1", sha)
			if perr == nil {
				if p := strings.TrimSpace(string(peeled)); p != "" {
					sha = p
				}
			}
			refs = append(refs, Ref{Kind: model.KindTag, Name: name, Commit: sha})
		}
	}
	return refs, nil
}

// CommitInfo describes one commit.
type CommitInfo struct {
	SHA     string
	Author  string
	Message string
	Time    time.Time
}

// ShowCommit reads metadata for a commit SHA using a NUL-delimited format.
func ShowCommit(ctx context.Context, repoPath, sha string) (CommitInfo, error) {
	out, err := run(ctx, repoPath, "show", "-s",
		"--format=%an%x00%aI%x00%s", sha)
	if err != nil {
		return CommitInfo{}, err
	}
	parts := strings.SplitN(strings.TrimRight(string(out), "\n"), "\x00", 3)
	ci := CommitInfo{SHA: sha}
	if len(parts) >= 1 {
		ci.Author = parts[0]
	}
	if len(parts) >= 2 {
		if t, perr := time.Parse(time.RFC3339, parts[1]); perr == nil {
			ci.Time = t
		}
	}
	if len(parts) >= 3 {
		ci.Message = parts[2]
	}
	return ci, nil
}
