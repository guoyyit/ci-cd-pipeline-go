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
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = w.Run(ctx, events) }()
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

	// Stop the polling loop before invoking Scan directly, then re-scan: it
	// must not re-emit the same refs (dedup by kind:ref:sha).
	cancel()
	<-runDone
	more, err := w.Scan(context.Background())
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

// TestWatcherFirstScanEstablishesBaselineOnly asserts the startup rule:
// the very first scan over a repo that already contains commits and tags
// establishes the baseline without emitting anything, even when Scan is
// called directly (not only via Run). Only refs appearing after that scan
// produce events.
func TestWatcherFirstScanEstablishesBaselineOnly(t *testing.T) {
	dir := newRepo(t)
	commit(t, dir, "history-a")
	commit(t, dir, "history-b")
	gitCmd(t, dir, "tag", "v1.0.0")
	gitCmd(t, dir, "tag", "v1.1.0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWatcher(dir, 20*time.Millisecond, nil)
	first, err := w.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 0 {
		t.Fatalf("baseline scan emitted %d historical events: %+v", len(first), first)
	}

	// A repeated scan with no repo changes must still emit nothing.
	again, err := w.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("repeat scan emitted %d events: %+v", len(again), again)
	}

	// Only post-startup changes are observable.
	commit(t, dir, "after-start")
	gitCmd(t, dir, "tag", "v2.0.0")
	got, err := w.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[model.Kind]int{}
	for _, ev := range got {
		kinds[ev.Kind]++
	}
	if kinds[model.KindPush] != 1 || kinds[model.KindTag] != 1 {
		t.Fatalf("want exactly 1 push and 1 tag after baseline, got %+v (%+v)", kinds, got)
	}

	// Repeated scans must not deliver the same event twice.
	dup, err := w.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(dup) != 0 {
		t.Fatalf("duplicate scan emitted %d events: %+v", len(dup), dup)
	}
}
