package git

import (
	"context"
	"fmt"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// Watcher polls a local Git repository and emits events when branch heads
// move or tags appear. The dedup key for a ref event is "<kind>:<name>:<sha>"
// so re-scanning the same ref is a no-op and force-pushes / new tags are seen.
type Watcher struct {
	repoPath string
	interval time.Duration
	seen     map[string]bool // key -> already emitted
	primed   bool            // whether the baseline scan has happened
}

// DefaultWatchInterval is used by NewWatcher when interval <= 0.
const DefaultWatchInterval = 2 * time.Second

// NewWatcher creates a Watcher for repoPath. prime (may be nil) seeds the set
// of already-known refs so the first scan does not replay existing history;
// when nil the first scan primes silently instead of emitting anything:
// commits and tags that already exist before startup only establish the
// baseline and never produce runs.
func NewWatcher(repoPath string, interval time.Duration, prime map[string]bool) *Watcher {
	if interval <= 0 {
		interval = DefaultWatchInterval
	}
	seen := make(map[string]bool, len(prime))
	for k := range prime {
		seen[k] = true
	}
	return &Watcher{repoPath: repoPath, interval: interval, seen: seen, primed: len(prime) > 0}
}

func refKey(r Ref) string {
	return string(r.Kind) + ":" + r.Name + ":" + r.Commit
}

// EventFromRef builds a model.Event with commit metadata for a changed ref.
func EventFromRef(ctx context.Context, repoPath string, r Ref, seenAt time.Time) (model.Event, error) {
	ev := model.Event{
		ID:        refKey(r),
		Kind:      r.Kind,
		Repo:      repoPath,
		Commit:    r.Commit,
		Timestamp: seenAt,
		SeenAt:    seenAt,
	}
	if r.Kind == model.KindPush {
		ev.Branch = r.Name
	} else {
		ev.Tag = r.Name
	}
	ci, err := ShowCommit(ctx, repoPath, r.Commit)
	if err == nil {
		ev.Author = ci.Author
		ev.Message = ci.Message
		if !ci.Time.IsZero() {
			ev.Timestamp = ci.Time
		}
	}
	return ev, nil
}

// Scan lists refs once and returns events for refs not in the seen set,
// updating the seen set as a side effect.
func (w *Watcher) Scan(ctx context.Context) ([]model.Event, error) {
	refs, err := ScanRefs(ctx, w.repoPath)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var events []model.Event
	for _, r := range refs {
		key := refKey(r)
		// Baseline scan: every pre-existing ref is recorded silently.
		if !w.primed {
			w.seen[key] = true
			continue
		}
		if !w.seen[key] {
			ev, err := EventFromRef(ctx, w.repoPath, r, now)
			if err != nil {
				return nil, fmt.Errorf("build event for %s: %w", key, err)
			}
			events = append(events, ev)
			w.seen[key] = true
		}
	}
	w.primed = true
	return events, nil
}

// Run polls until ctx is canceled, sending newly observed events to out.
// The very first scan only primes the seen set (no replay); subsequent scans
// emit newly appeared refs. It returns nil when ctx is canceled.
func (w *Watcher) Run(ctx context.Context, out chan<- model.Event) error {
	// Prime on first scan without emitting.
	if _, err := w.Scan(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			events, err := w.Scan(ctx)
			if err != nil {
				// Transient git errors are retried on the next tick; report
				// only when the context expired.
				if ctx.Err() != nil {
					return nil
				}
				continue
			}
			for _, ev := range events {
				select {
				case out <- ev:
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}
