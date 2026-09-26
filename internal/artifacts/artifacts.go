// Package artifacts collects task output files into a per-run artifact store
// after a task succeeds. Files are copied (preserving the relative directory
// layout) so deleting or modifying files in the work tree never corrupts a
// recorded run. The persisted TaskState stores the collected relative paths.
package artifacts

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Collector stores artifacts under Root/<run-id>/<stage>/<task>/.
type Collector struct {
	Root string
}

// New constructs a Collector rooted at dataRoot/artifacts.
func New(dataRoot string) *Collector {
	return &Collector{Root: filepath.Join(dataRoot, "artifacts")}
}

// Collect expands patterns (relative to workdir) and copies every match into
// <root>/<runID>/<stage>/<task>/, returning the stored relative paths.
// Missing patterns are not an error unless strict is set.
func (c *Collector) Collect(runID, stage, task, workdir string, patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	dest := filepath.Join(c.Root, runID, sanitize(stage), sanitize(task))
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact dir: %w", err)
	}

	var collected []string
	seen := map[string]bool{}
	for _, pat := range patterns {
		base := workdir
		if !filepath.IsAbs(pat) {
			pat = filepath.Join(workdir, pat)
		}
		matches, err := filepath.Glob(pat)
		if err != nil {
			return nil, fmt.Errorf("invalid artifact pattern %q: %w", pat, err)
		}
		for _, src := range matches {
			rel, err := filepath.Rel(base, src)
			if err != nil {
				rel = filepath.Base(src)
			}
			key := filepath.ToSlash(rel)
			if seen[key] {
				continue
			}
			seen[key] = true
			target := filepath.Join(dest, rel)
			if err := copyPath(src, target); err != nil {
				return collected, fmt.Errorf("collect %s: %w", rel, err)
			}
			collected = append(collected, key)
		}
	}
	return collected, nil
}

func sanitize(p string) string {
	p = strings.ReplaceAll(p, "..", "_")
	return strings.ReplaceAll(p, string(filepath.Separator), "_")
}

func copyPath(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return copyDir(src, dst)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
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
	return nil
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		return copyPath(path, target)
	})
}
