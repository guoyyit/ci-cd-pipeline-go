package pipeline

import (
	"testing"
	"time"
)

func testConfig() *Config {
	c := &Config{
		Name: "test",
		Repo: RepoConfig{Path: "/tmp/repo"},
		Trigger: TriggerConfig{
			Branches: []string{"*"},
			Tags:     []string{"v*"},
		},
		Settings: Settings{DataDir: "/tmp/data", MaxConcurrentRuns: 2,
			RunTimeout: Duration{time.Minute}},
		Stages: []StageConfig{
			{Name: "build", Tasks: []TaskConfig{{Name: "t1", Run: "true"}}},
			{Name: "test", Needs: []string{"build"}, Tasks: []TaskConfig{{Name: "t1", Run: "true"}}},
			{Name: "deploy", Needs: []string{"test"},
				Approval: &ApprovalConfig{Timeout: Duration{time.Hour}},
				Tasks:    []TaskConfig{{Name: "t1", Run: "true"}}},
			{Name: "notify", Needs: []string{"deploy"}, Tasks: []TaskConfig{{Name: "t1", Run: "true"}}},
		},
	}
	return c
}

func TestRunnableStagesDeps(t *testing.T) {
	cfg := testConfig()
	run := NewRun(cfg, Event{Type: EventPush, Ref: "main", Commit: "a"}, "r1")

	got := names(RunnableStages(run, cfg))
	if len(got) != 1 || got[0] != "build" {
		t.Fatalf("initial runnable = %v, want [build]", got)
	}

	run.Stages["build"].Status = StatusSuccess
	if got := names(RunnableStages(run, cfg)); len(got) != 1 || got[0] != "test" {
		t.Fatalf("after build success runnable = %v, want [test]", got)
	}
}

func TestFailedStageSkipsDownstream(t *testing.T) {
	cfg := testConfig()
	run := NewRun(cfg, Event{Type: EventPush, Ref: "main", Commit: "a"}, "r1")
	run.Stages["build"].Status = StatusFailed
	if !Evaluate(run, cfg) {
		t.Fatal("Evaluate should mark downstream stages")
	}
	// 传递跳过：test -> deploy(approval) -> notify
	for _, name := range []string{"test", "deploy", "notify"} {
		if run.Stages[name].Status != StatusSkipped {
			t.Fatalf("stage %s = %s, want skipped", name, run.Stages[name].Status)
		}
	}
	RecomputeRunStatus(run)
	if run.Status != StatusFailed {
		t.Fatalf("run status = %s, want failed", run.Status)
	}
}

func TestApprovalGateHoldsAndReleases(t *testing.T) {
	cfg := testConfig()
	run := NewRun(cfg, Event{Type: EventPush, Ref: "main", Commit: "a"}, "r1")
	run.Stages["build"].Status = StatusSuccess
	run.Stages["test"].Status = StatusSuccess

	if !Evaluate(run, cfg) {
		t.Fatal("deploy should enter waiting")
	}
	if run.Stages["deploy"].Status != StatusWaiting {
		t.Fatalf("deploy = %s, want waiting", run.Stages["deploy"].Status)
	}
	if run.Stages["deploy"].Approval.Deadline == nil {
		t.Fatal("approval deadline not set")
	}
	// 审批通过前 deploy 不可运行
	if got := RunnableStages(run, cfg); len(got) != 0 {
		t.Fatalf("runnable before approval = %v, want []", names(got))
	}
	run.Stages["deploy"].Approval.Decided = true
	run.Stages["deploy"].Approval.Approved = true
	run.Stages["deploy"].Status = StatusPending
	got := names(RunnableStages(run, cfg))
	if len(got) != 1 || got[0] != "deploy" {
		t.Fatalf("runnable after approval = %v, want [deploy]", got)
	}
}

func TestResetForRetryOnlyFailedAndDownstream(t *testing.T) {
	cfg := testConfig()
	run := NewRun(cfg, Event{Type: EventPush, Ref: "main", Commit: "a"}, "r1")
	run.Stages["build"].Status = StatusSuccess
	run.Stages["test"].Status = StatusFailed
	run.Stages["deploy"].Status = StatusSkipped
	run.Stages["notify"].Status = StatusSkipped

	ResetForRetry(run, cfg, false)
	if run.Stages["build"].Status != StatusSuccess {
		t.Fatalf("successful build must not be reset, got %s", run.Stages["build"].Status)
	}
	for _, n := range []string{"test", "deploy", "notify"} {
		if run.Stages[n].Status != StatusPending {
			t.Fatalf("stage %s = %s, want pending", n, run.Stages[n].Status)
		}
	}
	if run.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", run.Attempt)
	}
}

func TestResetForRetryForceRerunsAll(t *testing.T) {
	cfg := testConfig()
	run := NewRun(cfg, Event{Type: EventPush, Ref: "main", Commit: "a"}, "r1")
	run.Stages["build"].Status = StatusSuccess
	ResetForRetry(run, cfg, true)
	for _, n := range []string{"build", "test", "deploy", "notify"} {
		if run.Stages[n].Status != StatusPending {
			t.Fatalf("stage %s = %s, want pending under force", n, run.Stages[n].Status)
		}
	}
}

func TestDownstreamStages(t *testing.T) {
	cfg := testConfig()
	ds := DownstreamStages(cfg, "build")
	if !ds["test"] || !ds["deploy"] || !ds["notify"] {
		t.Fatalf("downstream of build = %v", ds)
	}
	if len(ds) != 3 {
		t.Fatalf("downstream size = %d, want 3", len(ds))
	}
}

func TestMatchAny(t *testing.T) {
	if !MatchAny([]string{"main", "release/*"}, "release/1.0") {
		t.Fatal("glob should match")
	}
	if MatchAny([]string{"main"}, "dev") {
		t.Fatal("should not match")
	}
	if MatchAny(nil, "main") {
		t.Fatal("empty rules must not match")
	}
}

func TestConfigValidateCycle(t *testing.T) {
	cfg := testConfig()
	cfg.Stages[0].Needs = []string{"test"} // build <- test <- build
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected cycle error")
	}
}

func names(stages []*StageRun) []string {
	var out []string
	for _, s := range stages {
		out = append(out, s.Name)
	}
	return out
}
