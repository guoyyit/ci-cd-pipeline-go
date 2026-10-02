package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/config"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/engine"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/git"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

// TestStartupBaselineCreatesNoRuns starts a fully wired daemon (watcher +
// ingest loop + HTTP API) against a repository that already contains several
// historical commits and tags. The first scan must only establish a
// baseline: zero events must be delivered and no runs created, even after
// several poll intervals elapse.
func TestStartupBaselineCreatesNoRuns(t *testing.T) {
	repoDir := initGitRepo(t)
	// Pre-existing history: commits and tags before the daemon starts.
	commitFile(t, repoDir, "old-1.txt")
	commitFile(t, repoDir, "old-2.txt")
	commitFile(t, repoDir, "old-3.txt")
	runGit(t, repoDir, "tag", "v1.0.0")
	runGit(t, repoDir, "tag", "v1.1.0")

	dataDir := t.TempDir()
	st, err := store.NewFileStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &model.Config{
		Name:     "baseline",
		Repo:     repoDir,
		Triggers: model.Triggers{Branches: []string{"main"}, Tags: []string{"v*"}},
		Stages:   []model.Stage{{Name: "noop", Tasks: []model.Task{{Name: "ok", Shell: true, Command: "true"}}}},
	}
	if err := config.Validate(cfg); err != nil {
		t.Fatal(err)
	}

	eng := engine.New(engine.Options{Store: st, WorkDir: repoDir})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Recover(context.Background(), engine.RecoveryResume); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// LIFO cleanups: stop the watcher/consumer first, then the engine, so
	// nothing calls into the engine while it is shutting down.
	t.Cleanup(func() {
		ctx2, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = eng.Stop(ctx2)
	})

	// Count only events observed after startup, exactly like pipelined.
	events := make(chan model.Event, 32)
	w := git.NewWatcher(repoDir, 30*time.Millisecond, nil)
	go func() { _ = w.Run(ctx, events) }()
	var eventCount int32
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				atomic.AddInt32(&eventCount, 1)
				_, _, _ = eng.TriggerEvent(context.Background(), cfg, ev)
			}
		}
	}()
	// Registered after the engine cleanup (LIFO): this runs first and waits
	// for the consumer to stop before the engine is torn down.
	t.Cleanup(func() {
		cancel()
		<-consumerDone
	})

	srv := httptest.NewServer(New(eng).Handler())
	defer srv.Close()

	// Let several polling intervals pass over the unchanged historical repo.
	time.Sleep(400 * time.Millisecond)

	resp, err := srv.Client().Get(srv.URL + "/runs")
	if err != nil {
		t.Fatal(err)
	}
	var runs []model.Run
	if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := atomic.LoadInt32(&eventCount); got != 0 {
		t.Fatalf("first scan emitted %d historical events, want 0", got)
	}
	if len(runs) != 0 {
		t.Fatalf("historical refs created %d runs, want 0", len(runs))
	}

	// Sanity: a new post-startup event does trigger exactly one run.
	commitFile(t, repoDir, "new.txt")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := srv.Client().Get(srv.URL + "/runs")
		if err != nil {
			t.Fatal(err)
		}
		var rs []model.Run
		_ = json.NewDecoder(resp.Body).Decode(&rs)
		resp.Body.Close()
		if len(rs) >= 1 {
			if len(rs) != 1 {
				t.Fatalf("post-startup event created %d runs, want 1", len(rs))
			}
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatal("post-startup commit did not trigger a run")
}
