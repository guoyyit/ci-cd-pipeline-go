package approval

import (
	"context"
	"testing"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

func TestApproveDecisionRecorded(t *testing.T) {
	m := NewManager()
	ctx := context.Background()
	rule := &model.ApprovalRule{Required: true, Approvers: []string{"alice"}}
	st, err := m.Request(ctx, "run-1", "deploy", rule)
	if err != nil || st.Status != model.StatusPending {
		t.Fatalf("Request: %+v, %v", st, err)
	}
	if !Allowed(rule, "alice") || Allowed(rule, "bob") {
		t.Fatal("Allowed check wrong")
	}
	if _, err := m.Approve(ctx, "run-1", "deploy", "alice"); err != nil {
		t.Fatalf("Approve: %v", err)
	}
	final, err := m.Wait(ctx, "run-1", "deploy")
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if final.Status != model.StatusSuccess || final.Approver != "alice" || final.DecidedAt == nil {
		t.Fatalf("bad final state: %+v", final)
	}
}

func TestRejectRequiresCommentAndIsAuditable(t *testing.T) {
	m := NewManager()
	ctx := context.Background()
	rule := &model.ApprovalRule{Required: true}
	if _, err := m.Request(ctx, "r", "s", rule); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Reject(ctx, "r", "s", "alice", "  "); err != ErrComment {
		t.Fatalf("blank comment: want ErrComment, got %v", err)
	}
	st, err := m.Reject(ctx, "r", "s", "bob", "change looks risky")
	if err != nil {
		t.Fatalf("Reject: %v", err)
	}
	if st.Status != model.StatusFailed || st.Approver != "bob" || st.Comment != "change looks risky" || st.DecidedAt == nil {
		t.Fatalf("rejection not auditable: %+v", st)
	}
	// Double decision is rejected.
	if _, err := m.Approve(ctx, "r", "s", "bob"); err != ErrDecided {
		t.Fatalf("want ErrDecided, got %v", err)
	}
}

func TestApprovalTimeoutCanceled(t *testing.T) {
	m := NewManager()
	ctx := context.Background()
	rule := &model.ApprovalRule{Required: true, Timeout: model.Duration(50 * time.Millisecond)}
	if _, err := m.Request(ctx, "r", "s", rule); err != nil {
		t.Fatal(err)
	}
	st, err := m.Wait(ctx, "r", "s")
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if st.Status != model.StatusCanceled || st.Comment == "" || st.DecidedAt == nil {
		t.Fatalf("timeout should cancel with audit: %+v", st)
	}
}

func TestApprovalWaitContextCancel(t *testing.T) {
	m := NewManager()
	rule := &model.ApprovalRule{Required: true}
	if _, err := m.Request(context.Background(), "r", "s", rule); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	st, err := m.Wait(ctx, "r", "s")
	if err == nil {
		t.Fatal("want context error")
	}
	if st.Status != model.StatusCanceled {
		t.Fatalf("want canceled, got %q", st.Status)
	}
}
