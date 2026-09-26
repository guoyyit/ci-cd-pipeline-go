package event

import (
	"context"
	"testing"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

func TestMatch(t *testing.T) {
	tr := model.Triggers{Branches: []string{"main", "feat/*"}, Tags: []string{"v[0-9]*"}}
	mainEv := model.Event{Kind: model.KindPush, Branch: "feat/x"}
	if !Match(tr, mainEv) {
		t.Fatal("feat/x should match")
	}
	if Match(tr, model.Event{Kind: model.KindPush, Branch: "other"}) {
		t.Fatal("other must not match")
	}
	if !Match(tr, model.Event{Kind: model.KindTag, Tag: "v1"}) {
		t.Fatal("v1 tag should match")
	}
}

func TestAdmitDeduplicates(t *testing.T) {
	st, err := store.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ev := model.Event{ID: "push:main:deadbeef", Kind: model.KindPush, Commit: "deadbeef"}

	ok, err := Admit(ctx, st, NewRecord(ev, "run-1"))
	if err != nil || !ok {
		t.Fatalf("first admit = %v, %v", ok, err)
	}
	ok, err = Admit(ctx, st, NewRecord(ev, "run-2"))
	if err != nil || ok {
		t.Fatalf("duplicate admit = %v, %v", ok, err)
	}
}
