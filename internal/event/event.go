// Package event matches Git events against pipeline trigger filters and
// provides de-duplication so the same event never creates two runs.
package event

import (
	"context"
	"path"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

// Match reports whether ev passes the trigger rules.
// Branch events must match one of t.Branches glob patterns, tag events one
// of t.Tags. Empty pattern lists mean that event kind never triggers.
func Match(t model.Triggers, ev model.Event) bool {
	patterns := t.Branches
	if ev.Kind == model.KindTag {
		patterns = t.Tags
	}
	name := ev.Branch
	if ev.Kind == model.KindTag {
		name = ev.Tag
	}
	if name == "" {
		return false
	}
	for _, p := range patterns {
		if ok, err := path.Match(p, name); err == nil && ok {
			return true
		}
	}
	return false
}

// Admit atomically checks the dedup record and creates it. It returns
// admitted=true only for the first occurrence of ev.ID; a duplicate event
// yields (false, nil) without error.
func Admit(ctx context.Context, st store.Store, rec *model.EventRecord) (bool, error) {
	exists, err := st.HasEvent(ctx, rec.EventID)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	if rec.SeenAt.IsZero() {
		rec.SeenAt = time.Now()
	}
	if err := st.RecordEvent(ctx, rec); err != nil {
		if err == store.ErrEventExists {
			return false, nil // concurrent duplicate
		}
		return false, err
	}
	return true, nil
}

// NewRecord builds the EventRecord persisted for an admitted event.
func NewRecord(ev model.Event, runID string) *model.EventRecord {
	return &model.EventRecord{
		EventID: ev.ID,
		Kind:    ev.Kind,
		Repo:    ev.Repo,
		Commit:  ev.Commit,
		Branch:  ev.Branch,
		Tag:     ev.Tag,
		RunID:   runID,
		SeenAt:  ev.SeenAt,
	}
}
