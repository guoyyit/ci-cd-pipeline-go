// Package store persists runs, stage/task states and event dedup records.
// The implementation is file based with atomic writes (temp file + rename),
// which is sufficient for a single local daemon and makes crash recovery
// straightforward: every state transition survives process restarts.
package store

import (
	"context"
	"errors"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// ErrNotFound is returned when a requested run does not exist.
var ErrNotFound = errors.New("store: not found")

// ErrEventExists is returned when an event dedup record already exists.
var ErrEventExists = errors.New("store: event already recorded")

// Store is the persistence abstraction implemented by the file-based store.
type Store interface {
	// SaveRun atomically persists the full run state.
	SaveRun(ctx context.Context, run *model.Run) error
	// LoadRun reads one run by id.
	LoadRun(ctx context.Context, id string) (*model.Run, error)
	// ListRuns returns all persisted runs, newest first.
	ListRuns(ctx context.Context) ([]*model.Run, error)
	// RecordEvent persists an event dedup record. It returns ErrEventExists
	// when the same event ID has already been recorded.
	RecordEvent(ctx context.Context, rec *model.EventRecord) error
	// HasEvent reports whether an event ID was already recorded.
	HasEvent(ctx context.Context, eventID string) (bool, error)
	// Close releases underlying resources.
	Close() error
	// WaitForWrites blocks until all in-flight writes have completed.
	// Implementations without write tracking may return immediately.
	WaitForWrites()
}
