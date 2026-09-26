package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/config"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/engine"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/git"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

// TestEndToEndGitTriggerApproveAndSuccess builds a real git repo with an
// approval-gated pipeline, starts the HTTP server + git watcher, pushes a
// commit via local git, then approves through the API and asserts success.
func TestEndToEndGitTriggerApproveAndSuccess(t *testing.T) {
	if !git.IsRepo(context.Background(), ".") {
		// not a git repo check unrelated; the real repo is created below
	}

	repoDir := initGitRepo(t)
	dataDir := t.TempDir()

	cfg := &model.Config{
		Name:     "e2e",
		Repo:     repoDir,
		Triggers: model.Triggers{Branches: []string{"main"}},
		Stages: []model.Stage{
			{Name: "build", Tasks: []model.Task{{Name: "ok", Shell: true, Command: "true"}}},
			{Name: "deploy", Needs: []string{"build"},
				Approval: &model.ApprovalRule{Required: true},
				Tasks:    []model.Task{{Name: "ok", Shell: true, Command: "true"}}},
		},
	}
	if err := config.Validate(cfg); err != nil {
		t.Fatal(err)
	}

	st, err := store.NewFileStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(engine.Options{Store: st, WorkDir: repoDir})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Recover(context.Background(), engine.RecoveryResume); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})

	// Watch + ingest loop, same wiring as pipelined.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan model.Event, 16)
	w := git.NewWatcher(repoDir, 50*time.Millisecond, nil)
	go func() { _ = w.Run(ctx, events) }()
	go func() {
		for ev := range events {
			_, _, _ = eng.TriggerEvent(ctx, cfg, ev)
		}
	}()

	// Make a new commit that the watcher should observe.
	commitFile(t, repoDir, "a.txt")

	srv := httptest.NewServer(New(eng).Handler())
	defer srv.Close()
	client := srv.Client()

	// Wait for a run to appear via the list API.
	var runID string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(srv.URL + "/runs")
		if err != nil {
			t.Fatal(err)
		}
		var runs []model.Run
		_ = json.NewDecoder(resp.Body).Decode(&runs)
		resp.Body.Close()
		if len(runs) == 1 {
			runID = runs[0].ID
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if runID == "" {
		t.Fatal("no run was triggered from the git commit")
	}

	// Wait until the deploy stage is pending approval.
	waitRun(t, client, srv.URL+"/runs/"+runID, func(r *model.Run) bool {
		return r.Stage("deploy") != nil && r.Stage("deploy").Status == model.StatusPending &&
			r.Stage("build").Status == model.StatusSuccess
	})

	// Approve through the API.
	body := `{"stage":"deploy","approver":"alice"}`
	resp, err := client.Post(srv.URL+"/runs/"+runID+"/approve", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("approve status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	final := waitRun(t, client, srv.URL+"/runs/"+runID, func(r *model.Run) bool { return r.Status.IsTerminal() })
	if final.Status != model.StatusSuccess {
		t.Fatalf("final status = %s (%s)", final.Status, final.Reason)
	}

	// A second commit on a non-matching ref pattern would be filtered; here
	// instead verify duplicate handling by replaying the same event directly.
	refs, _ := git.ScanRefs(context.Background(), repoDir)
	var ev model.Event
	for _, r := range refs {
		if r.Kind == model.KindPush && r.Name == "main" {
			ev, _ = git.EventFromRef(context.Background(), repoDir, r, time.Now())
		}
	}
	_, created, err := eng.TriggerEvent(context.Background(), cfg, ev)
	if err != nil || created {
		t.Fatalf("replayed event must be deduplicated: created=%v err=%v", created, err)
	}
}

func waitRun(t *testing.T, client *http.Client, url string, pred func(*model.Run) bool) *model.Run {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		var r model.Run
		_ = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		if pred(&r) {
			return &r
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatal("run predicate never satisfied")
	return nil
}

func TestCancelViaHTTP(t *testing.T) {
	repoDir := initGitRepo(t)
	st, _ := store.NewFileStore(t.TempDir())
	eng := engine.New(engine.Options{Store: st, WorkDir: repoDir})
	_ = eng.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	cfg := &model.Config{
		Name: "c", Repo: repoDir,
		Triggers: model.Triggers{Branches: []string{"main"}},
		Stages:   []model.Stage{{Name: "long", Tasks: []model.Task{{Name: "s", Shell: true, Command: "sleep 30"}}}},
	}
	id, _, err := eng.TriggerEvent(context.Background(), cfg, model.Event{
		ID: "push:main:x", Kind: model.KindPush, Branch: "main", Commit: "x",
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(eng).Handler())
	defer srv.Close()
	resp, err := srv.Client().Post(srv.URL+"/runs/"+id+"/cancel", "application/json",
		bytes.NewBufferString(`{"reason":"api abort"}`))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel: status=%d err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()
	final := waitRun(t, srv.Client(), srv.URL+"/runs/"+id, func(r *model.Run) bool { return r.Status.IsTerminal() })
	if final.Status != model.StatusCanceled {
		t.Fatalf("status = %s", final.Status)
	}
}
