// Package approval implements manual approval gates. Every decision
// (approve / reject / timeout-cancel) is recorded in an ApprovalState that
// the engine persists with the run, giving an auditable trail: who decided,
// when, and (for rejections) a mandatory comment.
package approval

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// Errors returned by the Manager.
var (
	ErrNotFound = errors.New("approval: request not found")
	ErrDecided  = errors.New("approval: request already decided")
	ErrApprover = errors.New("approval: approver not allowed")
	ErrComment  = errors.New("approval: rejection comment is required")
)

// request is one live (in-memory) approval gate.
type request struct {
	state    *model.ApprovalState
	ch       chan struct{} // closed when the decision is final
	deadline *time.Timer
}

func key(runID, stage string) string { return runID + "/" + stage }

// Manager tracks pending approval requests for the lifetime of the daemon.
// Persistent state lives in the run itself; the manager only coordinates
// goroutines waiting on a decision.
type Manager struct {
	mu   sync.Mutex
	live map[string]*request
}

// NewManager constructs a Manager.
func NewManager() *Manager { return &Manager{live: make(map[string]*request)} }

// Request opens an approval request for runID/stageName. If an in-memory
// request already exists it is returned (used after resume).
func (m *Manager) Request(_ context.Context, runID, stageName string, rule *model.ApprovalRule) (*model.ApprovalState, error) {
	now := time.Now()
	st := &model.ApprovalState{
		Required:  rule != nil && rule.Required,
		Status:    model.StatusPending,
		CreatedAt: now,
	}
	if rule != nil && rule.Timeout.Std() > 0 {
		dl := now.Add(rule.Timeout.Std())
		st.Deadline = &dl
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(runID, stageName)
	if existing, ok := m.live[k]; ok {
		return cloneState(existing.state), nil
	}
	req := &request{state: st, ch: make(chan struct{})}
	m.live[k] = req
	return cloneState(st), nil
}

// Adopt re-registers an approval request that already exists in persistent
// state (process restart case) so that Wait/Approve/Reject work again.
// Already-decided states are not adopted.
func (m *Manager) Adopt(_ context.Context, runID, stageName string, st *model.ApprovalState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(runID, stageName)
	if _, ok := m.live[k]; ok {
		return
	}
	if st == nil || st.Status != model.StatusPending {
		return
	}
	m.live[k] = &request{state: cloneState(st), ch: make(chan struct{})}
}

// Peek returns the current state of a registered request without blocking.
func (m *Manager) Peek(runID, stage string) (*model.ApprovalState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.live[key(runID, stage)]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneState(req.state), nil
}

func (m *Manager) get(runID, stage string) (*request, bool) {
	req, ok := m.live[key(runID, stage)]
	return req, ok
}

// Allowed reports whether approver is permitted by rule (empty list = any).
func Allowed(rule *model.ApprovalRule, approver string) bool {
	if rule == nil || len(rule.Approvers) == 0 {
		return true
	}
	for _, a := range rule.Approvers {
		if a == approver {
			return true
		}
	}
	return false
}

// Approve grants an approval. approver is recorded for audit.
func (m *Manager) Approve(_ context.Context, runID, stage, approver string) (*model.ApprovalState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.get(runID, stage)
	if !ok {
		return nil, ErrNotFound
	}
	if req.state.Status != model.StatusPending {
		return req.state, ErrDecided
	}
	if approver == "" {
		return nil, errors.New("approval: approver must not be empty")
	}
	// Approver list enforcement is performed by the engine against the rule;
	// the manager itself accepts any named approver so recovery paths work.
	now := time.Now()
	req.state.Status = model.StatusSuccess
	req.state.Approver = approver
	req.state.DecidedAt = &now
	close(req.ch)
	return cloneState(req.state), nil
}

// Reject denies an approval. A non-empty comment is mandatory and recorded.
func (m *Manager) Reject(_ context.Context, runID, stage, approver, comment string) (*model.ApprovalState, error) {
	if strings.TrimSpace(comment) == "" {
		return nil, ErrComment
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.get(runID, stage)
	if !ok {
		return nil, ErrNotFound
	}
	if req.state.Status != model.StatusPending {
		return req.state, ErrDecided
	}
	if approver == "" {
		return nil, errors.New("approval: approver must not be empty")
	}
	now := time.Now()
	req.state.Status = model.StatusFailed
	req.state.Approver = approver
	req.state.Comment = strings.TrimSpace(comment)
	req.state.DecidedAt = &now
	close(req.ch)
	return cloneState(req.state), nil
}

// Cancel marks a pending request canceled (timeout, run cancel or shutdown).
func (m *Manager) Cancel(runID, stage, reason string) *model.ApprovalState {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.get(runID, stage)
	if !ok || req.state.Status != model.StatusPending {
		if ok {
			return cloneState(req.state)
		}
		return nil
	}
	now := time.Now()
	req.state.Status = model.StatusCanceled
	req.state.Comment = reason
	req.state.DecidedAt = &now
	close(req.ch)
	return cloneState(req.state)
}

// Wait blocks until the request is decided, its deadline passes or ctx is
// canceled. It returns the final state.
func (m *Manager) Wait(ctx context.Context, runID, stage string) (*model.ApprovalState, error) {
	m.mu.Lock()
	req, ok := m.get(runID, stage)
	if !ok {
		m.mu.Unlock()
		return nil, ErrNotFound
	}
	ch := req.ch
	var deadlineCh <-chan time.Time
	if req.state.Deadline != nil {
		d := time.Until(*req.state.Deadline)
		if d <= 0 {
			d = 0
		}
		t := time.NewTimer(d)
		defer t.Stop()
		deadlineCh = t.C
	}
	m.mu.Unlock()

	select {
	case <-ch:
	case <-deadlineCh:
		m.Cancel(runID, stage, "approval timed out")
		st, _ := m.peek(runID, stage)
		return st, nil
	case <-ctx.Done():
		m.Cancel(runID, stage, "run canceled while waiting for approval")
		st, _ := m.peek(runID, stage)
		return st, ctx.Err()
	}
	st, _ := m.peek(runID, stage)
	return st, nil
}

func (m *Manager) peek(runID, stage string) (*model.ApprovalState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.live[key(runID, stage)]
	if !ok {
		return nil, false
	}
	return cloneState(req.state), true
}

// Remove deletes the in-memory coordination entry (called once a run finishes).
func (m *Manager) Remove(runID, stage string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.live, key(runID, stage))
}

func cloneState(s *model.ApprovalState) *model.ApprovalState {
	cp := *s
	return &cp
}
