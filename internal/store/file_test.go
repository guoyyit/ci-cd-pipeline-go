package store

import (
	"context"
	"testing"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

func TestFileStoreRunRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	ctx := context.Background()

	run := &model.Run{ID: "run-1", Pipeline: "p", Status: model.StatusRunning}
	if err := st.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun: %v", err)
	}
	got, err := st.LoadRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("LoadRun: %v", err)
	}
	if got.Status != model.StatusRunning {
		t.Fatalf("status = %q, want running", got.Status)
	}

	if _, err := st.LoadRun(ctx, "missing"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	runs, err := st.ListRuns(ctx)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ListRuns = %v, %v", runs, err)
	}
}

func TestFileStoreEventDedup(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	ctx := context.Background()
	rec := &model.EventRecord{EventID: "push:main:abc", RunID: "run-1"}

	if exists, _ := st.HasEvent(ctx, rec.EventID); exists {
		t.Fatal("event should not exist yet")
	}
	if err := st.RecordEvent(ctx, rec); err != nil {
		t.Fatalf("first RecordEvent: %v", err)
	}
	if err := st.RecordEvent(ctx, rec); err != ErrEventExists {
		t.Fatalf("second RecordEvent: want ErrEventExists, got %v", err)
	}
	if exists, _ := st.HasEvent(ctx, rec.EventID); !exists {
		t.Fatal("event should exist after record")
	}
}

func TestFileStoreWaitForWrites(t *testing.T) {
	st, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			_ = st.SaveRun(ctx, &model.Run{ID: "r", Status: model.StatusRunning})
		}
		close(done)
	}()
	st.WaitForWrites()
	<-done
	if _, err := st.LoadRun(ctx, "r"); err != nil {
		t.Fatalf("final write must be durable: %v", err)
	}
}
