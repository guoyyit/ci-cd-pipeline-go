package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestE2E_PushTriggersViaWatcherAndAPI 使用真实 Git 仓库：
// 监听器发现提交 → 运行流水线 → 通过 HTTP API 查询状态。
func TestE2E_PushTriggersViaWatcherAndAPI(t *testing.T) {
	repo := initRepo(t)
	cfg := simpleCfg(StageConfig{Name: "build", Tasks: []TaskConfig{
		{Name: "hello", Run: "echo building", Artifacts: []string{"f.txt"}},
	}})
	cfg.Repo.Path = repo
	cfg.Settings.DataDir = t.TempDir()
	cfg.Settings.MaxConcurrentRuns = 1
	cfg.Settings.RunTimeout = Duration{time.Minute}

	store, err := OpenStore(filepath.Join(cfg.Settings.DataDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	exec_ := NewExecutor(repo, cfg.Settings.DataDir)
	r := NewRunner(cfg, store, exec_)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	defer r.Stop()

	w := NewWatcher(repo, 20*time.Millisecond, store, func(e Event) {
		if _, _, err := r.SubmitEvent(e); err != nil {
			t.Errorf("submit: %v", err)
		}
	})
	go w.Run(ctx)

	srv := httptest.NewServer(NewServer(r).Handler())
	defer srv.Close()

	// 等待首轮基线建立
	time.Sleep(200 * time.Millisecond)

	sha := commitMore(t, repo, "2")

	var runID string
	waitFor(t, 5*time.Second, func() bool {
		resp, err := http.Get(srv.URL + "/api/runs")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var runs []Run
		if json.NewDecoder(resp.Body).Decode(&runs) != nil || len(runs) == 0 {
			return false
		}
		runID = runs[0].ID
		return runs[0].Event.Commit == sha
	}, "run created via watcher")

	waitFor(t, 5*time.Second, func() bool {
		resp, err := http.Get(srv.URL + "/api/runs/" + runID)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var run Run
		if json.NewDecoder(resp.Body).Decode(&run) != nil {
			return false
		}
		return run.Status == StatusSuccess
	}, "e2e run success")

	// 产物已收集
	art := filepath.Join(cfg.Settings.DataDir, "runs", runID, "artifacts", "build", "hello", "f.txt")
	if _, err := os.Stat(art); err != nil {
		t.Fatalf("artifact not collected: %v", err)
	}

	// 再次手动投递相同事件：不重复创建
	body, _ := json.Marshal(Event{Type: EventPush, Repo: repo, Ref: "main", Commit: sha})
	resp, err := http.Post(srv.URL+"/api/events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var dup struct {
		Created bool `json:"created"`
	}
	json.NewDecoder(resp.Body).Decode(&dup)
	if dup.Created {
		t.Fatal("duplicate event created a second run")
	}
}

// TestE2E_ApprovalViaHTTP 走 HTTP API 的审批流程。
func TestE2E_ApprovalViaHTTP(t *testing.T) {
	cfg := simpleCfg(
		StageConfig{Name: "build", Tasks: []TaskConfig{{Name: "t", Run: "true"}}},
		StageConfig{Name: "deploy", Needs: []string{"build"},
			Approval: &ApprovalConfig{Timeout: Duration{time.Hour}},
			Tasks:    []TaskConfig{{Name: "t", Run: "true"}}},
	)
	r, _, _ := newTestRunner(t, cfg)
	srv := httptest.NewServer(NewServer(r).Handler())
	defer srv.Close()

	run, _, _ := r.SubmitEvent(Event{Type: EventPush, Repo: "r", Ref: "main", Commit: "c9"})

	waitFor(t, 5*time.Second, func() bool {
		got, _ := r.GetRun(run.ID)
		return got.Stages["deploy"].Status == StatusWaiting
	}, "deploy waiting")

	// 拒绝必须带 reason
	resp, _ := http.Post(srv.URL+"/api/runs/"+run.ID+"/stages/deploy/reject",
		"application/json", bytes.NewReader([]byte(`{"approver":"bob"}`)))
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("reject without reason status = %d, want 409", resp.StatusCode)
	}

	// 审批通过
	resp, _ = http.Post(srv.URL+"/api/runs/"+run.ID+"/stages/deploy/approve",
		"application/json", bytes.NewReader([]byte(`{"approver":"alice"}`)))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("approve status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	waitFor(t, 5*time.Second, func() bool {
		return runStatus(t, r, run.ID) == StatusSuccess
	}, "approved run success")

	// 终态运行不能再次取消
	resp, _ = http.Post(srv.URL+"/api/runs/"+run.ID+"/cancel", "application/json", nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("cancel finished run status = %d, want 409", resp.StatusCode)
	}
}
