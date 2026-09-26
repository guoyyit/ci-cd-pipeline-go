package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

func validCfg() *model.Config {
	return &model.Config{
		Name: "p", Repo: "/tmp/repo",
		Triggers: model.Triggers{Branches: []string{"main"}},
		Stages: []model.Stage{
			{Name: "a", Tasks: []model.Task{{Name: "t1", Shell: true, Command: "true"}}},
			{Name: "b", Needs: []string{"a"}, Tasks: []model.Task{{Name: "t2", Shell: true, Command: "true"}}},
		},
	}
}

func TestValidateOK(t *testing.T) {
	if err := Validate(validCfg()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []func(*model.Config){
		func(c *model.Config) { c.Name = "" },
		func(c *model.Config) { c.Repo = "" },
		func(c *model.Config) { c.Triggers = model.Triggers{} },
		func(c *model.Config) { c.Stages = nil },
		func(c *model.Config) { c.Stages[0].Name = "" },
		func(c *model.Config) { c.Stages[0].Tasks = nil },
		func(c *model.Config) { c.Stages[0].Tasks[0].Command = "" },
		func(c *model.Config) { c.Stages[1].Needs = []string{"nope"} },
		func(c *model.Config) {
			c.Stages[0].Needs = []string{"b"}
		},
	}
	for i, mut := range cases {
		c := validCfg()
		mut(c)
		if err := Validate(c); err == nil {
			t.Fatalf("case %d: expected validation error", i)
		}
	}
}

func TestValidateCycle(t *testing.T) {
	c := validCfg()
	c.Stages = []model.Stage{
		{Name: "a", Needs: []string{"b"}, Tasks: []model.Task{{Name: "x", Command: "true"}}},
		{Name: "b", Needs: []string{"a"}, Tasks: []model.Task{{Name: "y", Command: "true"}}},
	}
	if err := Validate(c); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestLoadJSONWithDurations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.json")
	content := `{
	  "name":"p","repo":"/tmp/r","concurrency":1,"run_timeout":"30m",
	  "triggers":{"branches":["main","release/*"],"tags":["v*"]},
	  "stages":[
	    {"name":"a","timeout":"5s","retry":1,
	     "tasks":[{"name":"t","shell":true,"command":"true"}]},
	    {"name":"b","needs":["a"],
	     "approval":{"required":true,"approvers":["alice"],"timeout":"1h"},
	     "tasks":[{"name":"t","shell":true,"command":"true"}]}
	  ]
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(context.Background(), path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RunTimeout.Std().String() != "30m0s" {
		t.Fatalf("run timeout = %v", cfg.RunTimeout)
	}
	if cfg.Stages[0].Timeout.Std().String() != "5s" {
		t.Fatalf("stage timeout = %v", cfg.Stages[0].Timeout)
	}
	if cfg.Stages[1].Approval.Timeout.Std().String() != "1h0m0s" {
		t.Fatalf("approval timeout = %v", cfg.Stages[1].Approval.Timeout)
	}
}

func TestMatcher(t *testing.T) {
	m := NewMatcher(model.Triggers{
		Branches: []string{"main", "release/*"},
		Tags:     []string{"v*"},
	})
	cases := []struct {
		kind model.Kind
		ref  string
		want bool
	}{
		{model.KindPush, "main", true},
		{model.KindPush, "release/1.0", true},
		{model.KindPush, "feature/x", false},
		{model.KindTag, "v1.2.3", true},
		{model.KindTag, "beta", false},
	}
	for _, c := range cases {
		ev := model.Event{Kind: c.kind}
		if c.kind == model.KindPush {
			ev.Branch = c.ref
		} else {
			ev.Tag = c.ref
		}
		if got := m.Match(ev); got != c.want {
			t.Errorf("%s %s: got %v want %v", c.kind, c.ref, got, c.want)
		}
	}
}
