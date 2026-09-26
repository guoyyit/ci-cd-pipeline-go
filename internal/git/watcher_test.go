package git

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

func gitCmd(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, dir, err, out)
	}
}

func newRepo(t *testing.T) string {
	dir := t.TempDir()
	gitCmd(t, dir, "init", "-q", "-b", "main")
	gitCmd(t, dir, "config", "user.email", "t@example.com")
	gitCmd(t, dir, "config", "user.name", "tester")
	gitCmd(t, dir, "config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", ".")
	gitCmd(t, dir, "commit", "-q", "-m", "initial")
	return dir
}

func commit(t *testing.T, dir, msg string) {
	t.Helper()
	f := filepath.Join(dir, "f-"+msg)
	if err := os.WriteFile(f, []byte(msg), 0o644); err != nil {
		t.Fatal(err)
	}
	gitCmd(t, dir, "add", ".")
	gitCmd(t, dir, "commit", "-q", "-m", msg)
}

func TestScanRefsAndIsRepo(t *testing.T) {
	if !IsRepo(context.Background(), newRepo(t)) {
		t.Fatal("IsRepo should be true for a git repo")
	}
	if IsRepo(context.Background(), t.TempDir()) {
		t.Fatal("IsRepo should be false for a plain dir")
	}
	dir := newRepo(t)
	gitCmd(t, dir, "tag", "v1.0.0")

	refs, err := ScanRefs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range refs {
		got[string(r.Kind)+":"+r.Name] = r.Commit
	}
	if _, ok := got["push:main"]; !ok {
		t.Fatalf("main branch ref missing: %v", got)
	}
	if sha, ok := got["tag:v1.0.0"]; !ok || sha == "" {
		t.Fatalf("tag ref missing or unpeeled: %v", got)
	}
}

func TestWatcherEmitsNewCommitAndTagThenDedups(t *testing.T) {
	dir := newRepo(t)
	w := NewWatcher(dir, 30*time.Millisecond, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan model.Event, 8)
	go func() { _ = w.Run(ctx, events) }()
	// First scan primes; give it a moment.
	time.Sleep(150 * time.Millisecond)

	commit(t, dir, "one")
	commit(t, dir, "two")
	gitCmd(t, dir, "tag", "v2.0.0")

	var sawPush, sawTag bool
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !(sawPush && sawTag) {
		select {
		case ev := <-events:
			switch ev.Kind {
			case model.KindPush:
				if ev.Branch != "main" || ev.Commit == "" || ev.ID == "" {
					t.Fatalf("bad push event: %+v", ev)
				}
				sawPush = true
			case model.KindTag:
				if ev.Tag != "v2.0.0" {
					t.Fatalf("bad tag event: %+v", ev)
				}
				sawTag = true
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !sawPush || !sawTag {
		t.Fatalf("missing events push=%v tag=%v", sawPush, sawTag)
	}

	// Re-scan must not re-emit the same refs (dedup by kind:ref:sha).
	more, err := w.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(more) != 0 {
		t.Fatalf("re-scan produced duplicate events: %+v", more)
	}
}

func TestWatcherPrimeMapSuppressesHistory(t *testing.T) {
	dir := newRepo(t)
	refs, err := ScanRefs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	prime := map[string]bool{}
	for _, r := range refs {
		prime[refKey(r)] = true
	}
	w := NewWatcher(dir, 20*time.Millisecond, prime)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan model.Event, 8)
	go func() { _ = w.Run(ctx, events) }()
	select {
	case ev := <-events:
		t.Fatalf("primed watcher emitted an event: %+v", ev)
	case <-time.After(200 * time.Millisecond):
	}
}
