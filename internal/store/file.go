package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
)

// FileStore persists runs as JSON files under a data directory:
//
//	<dir>/runs/<run-id>.json
//	<dir>/events/<event-id>.json
//
// Writes go through a temp file and an atomic rename; event records are
// created with O_CREATE|O_EXCL so concurrent ingesters cannot both win.
type FileStore struct {
	dir string
	mu  sync.Mutex // serializes in-process writes
	wg  sync.WaitGroup
}

// NewFileStore opens (creating if needed) a file-backed store in dir.
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("store dir must not be empty")
	}
	for _, sub := range []string{"runs", "events"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("create store dir: %w", err)
		}
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) Close() error {
	s.wg.Wait()
	return nil
}

// WaitForWrites blocks until every started write has finished.
func (s *FileStore) WaitForWrites() { s.wg.Wait() }

// beginWrite/endWrite bracket an in-flight persisted write.
func (s *FileStore) beginWrite() { s.wg.Add(1) }
func (s *FileStore) endWrite()   { s.wg.Done() }

// writeJSONAtomic marshals v and writes it to path via temp file + rename.
func writeJSONAtomic(path string, v interface{}) (err error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}

func (s *FileStore) SaveRun(_ context.Context, run *model.Run) error {
	if run == nil || run.ID == "" {
		return fmt.Errorf("cannot save run without id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginWrite()
	defer s.endWrite()
	path := filepath.Join(s.dir, "runs", run.ID+".json")
	return writeJSONAtomic(path, run)
}

func (s *FileStore) LoadRun(_ context.Context, id string) (*model.Run, error) {
	path := filepath.Join(s.dir, "runs", id+".json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load run %s: %w", id, err)
	}
	run := &model.Run{}
	if err := json.Unmarshal(data, run); err != nil {
		return nil, fmt.Errorf("decode run %s: %w", id, err)
	}
	return run, nil
}

func (s *FileStore) ListRuns(_ context.Context) ([]*model.Run, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "runs"))
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	var runs []*model.Run
	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, "runs", ent.Name()))
		if err != nil {
			continue // tolerate a torn/garbage file
		}
		run := &model.Run{}
		if err := json.Unmarshal(data, run); err != nil {
			continue
		}
		runs = append(runs, run)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].CreatedAt.After(runs[j].CreatedAt) })
	return runs, nil
}

// RecordEvent stores the dedup record. O_EXCL guarantees that even with
// concurrent callers only one insert succeeds.
func (s *FileStore) RecordEvent(_ context.Context, rec *model.EventRecord) error {
	if rec == nil || rec.EventID == "" {
		return fmt.Errorf("cannot record event without id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginWrite()
	defer s.endWrite()
	path := filepath.Join(s.dir, "events", rec.EventID+".json")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, os.ErrExist) {
		return ErrEventExists
	}
	if err != nil {
		return fmt.Errorf("record event: %w", err)
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	return f.Close()
}

func (s *FileStore) HasEvent(_ context.Context, eventID string) (bool, error) {
	_, err := os.Stat(filepath.Join(s.dir, "events", eventID+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat event: %w", err)
	}
	return true, nil
}
