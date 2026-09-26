package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// storeData 是持久化到磁盘的顶层结构。
type storeData struct {
	Runs   map[string]*Run   `json:"runs"`
	Events map[string]Event  `json:"events"`
	Refs   map[string]string `json:"refs"`
}

// Store 负责运行状态与事件的持久化。
// 写操作通过 临时文件+rename 保证原子性，崩溃不会产生半截文件。
type Store struct {
	mu   sync.Mutex
	path string
	data storeData
}

// OpenStore 打开（必要时创建）持久化存储。
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path}
	s.data.Runs = map[string]*Run{}
	s.data.Events = map[string]Event{}
	s.data.Refs = map[string]string{}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return nil, err
			}
			return s, s.saveLocked()
		}
		return nil, fmt.Errorf("open store: %w", err)
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return nil, fmt.Errorf("corrupt store %s: %w", path, err)
	}
	if s.data.Runs == nil {
		s.data.Runs = map[string]*Run{}
	}
	if s.data.Events == nil {
		s.data.Events = map[string]Event{}
	}
	if s.data.Refs == nil {
		s.data.Refs = map[string]string{}
	}
	return s, nil
}

// Close 关闭存储（当前实现为无操作，保留作接口对称）。
func (s *Store) Close() error { return nil }

// saveLocked 将数据原子写盘，调用方必须持有锁。
func (s *Store) saveLocked() error {
	raw, err := json.MarshalIndent(&s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("commit store: %w", err)
	}
	return nil
}

// deepCopyRun 通过 JSON 往返深拷贝，隔离内存共享。
func deepCopyRun(r *Run) *Run {
	raw, err := json.Marshal(r)
	if err != nil {
		return r
	}
	var cp Run
	if err := json.Unmarshal(raw, &cp); err != nil {
		return r
	}
	return &cp
}

// HasEvent 报告事件是否已处理过（去重）。
func (s *Store) HasEvent(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data.Events[key]
	return ok
}

// RecordEvent 记录已处理事件。
func (s *Store) RecordEvent(e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Events[e.Key()] = e
	return s.saveLocked()
}

// SaveRun 保存运行状态（存储深拷贝）。
func (s *Store) SaveRun(r *Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Runs[r.ID] = deepCopyRun(r)
	return s.saveLocked()
}

// GetRun 按 ID 查询运行（返回深拷贝）。
func (s *Store) GetRun(id string) (*Run, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.data.Runs[id]
	if !ok {
		return nil, false
	}
	return deepCopyRun(r), true
}

// ListRuns 返回全部运行，按创建时间升序。
func (s *Store) ListRuns() []*Run {
	s.mu.Lock()
	defer s.mu.Unlock()
	runs := make([]*Run, 0, len(s.data.Runs))
	for _, r := range s.data.Runs {
		runs = append(runs, deepCopyRun(r))
	}
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].CreatedAt.Equal(runs[j].CreatedAt) {
			return runs[i].ID < runs[j].ID
		}
		return runs[i].CreatedAt.Before(runs[j].CreatedAt)
	})
	return runs
}

// IncompleteRuns 返回未完成的运行（用于崩溃恢复）。
func (s *Store) IncompleteRuns() []*Run {
	s.mu.Lock()
	defer s.mu.Unlock()
	var runs []*Run
	for _, r := range s.data.Runs {
		if !r.Status.Terminal() {
			runs = append(runs, deepCopyRun(r))
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].CreatedAt.Before(runs[j].CreatedAt) })
	return runs
}

// GetRef 返回仓库某引用的已知提交。
func (s *Store) GetRef(repo, name string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sha, ok := s.data.Refs[repo+" "+name]
	return sha, ok
}

// SetRef 记录仓库某引用的最新提交。
func (s *Store) SetRef(repo, name, commit string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Refs[repo+" "+name] = commit
	return s.saveLocked()
}
