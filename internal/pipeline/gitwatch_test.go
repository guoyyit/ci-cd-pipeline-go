package pipeline

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// initRepo 创建带一次提交的本地仓库。
func initRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", ".")
	run("commit", "-m", "init")
	return dir
}

func commitMore(t *testing.T, repo, content string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, "f.txt"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("git", "-C", repo, "commit", "-am", "update")
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("commit: %v\n%s", err, out)
	}
	out, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return string(out[:len(out)-1])
}

func TestWatcherDetectsPushAndTag(t *testing.T) {
	repo := initRepo(t)
	store, err := OpenStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	w := NewWatcher(repo, time.Second, store, nil)

	// 首次轮询：建立基线，不产生事件
	events, err := w.PollOnce()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("baseline poll produced %d events, want 0", len(events))
	}

	// 新提交 → push 事件
	sha := commitMore(t, repo, "2")
	events, err = w.PollOnce()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != EventPush || events[0].Ref != "main" || events[0].Commit != sha {
		t.Fatalf("push event = %+v", events)
	}

	// 无变化 → 无事件
	events, _ = w.PollOnce()
	if len(events) != 0 {
		t.Fatalf("no-change poll produced %d events", len(events))
	}

	// 新标签 → tag 事件
	cmd := exec.Command("git", "-C", repo, "tag", "v1.0.0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("tag: %v\n%s", err, out)
	}
	events, err = w.PollOnce()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != EventTag || events[0].Ref != "v1.0.0" {
		t.Fatalf("tag event = %+v", events)
	}
}

func TestWatcherStateSurvivesRestart(t *testing.T) {
	repo := initRepo(t)
	statePath := filepath.Join(t.TempDir(), "state.json")

	store1, _ := OpenStore(statePath)
	w1 := NewWatcher(repo, time.Second, store1, nil)
	if _, err := w1.PollOnce(); err != nil {
		t.Fatal(err)
	}
	store1.Close()

	// 重启后重新打开存储：不应重复产生基线事件
	store2, _ := OpenStore(statePath)
	defer store2.Close()
	w2 := NewWatcher(repo, time.Second, store2, nil)
	events, err := w2.PollOnce()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("after restart got %d events, want 0 (no duplicate)", len(events))
	}
}

func TestStorePersistsRunsAndEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s1, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	e := Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "abc", ReceivedAt: time.Now()}
	if err := s1.RecordEvent(e); err != nil {
		t.Fatal(err)
	}
	run := &Run{ID: "r1", Pipeline: "p", Event: e, Status: StatusRunning,
		Stages: map[string]*StageRun{}, CreatedAt: time.Now()}
	if err := s1.SaveRun(run); err != nil {
		t.Fatal(err)
	}
	s1.Close()

	s2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if !s2.HasEvent(e.Key()) {
		t.Fatal("event not persisted")
	}
	got, ok := s2.GetRun("r1")
	if !ok || got.Status != StatusRunning {
		t.Fatalf("run not persisted: %+v", got)
	}
	if n := len(s2.IncompleteRuns()); n != 1 {
		t.Fatalf("incomplete runs = %d, want 1", n)
	}
}
