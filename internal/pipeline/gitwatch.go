package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Watcher 轮询本地 Git 仓库的提交与标签事件。
type Watcher struct {
	repo     string
	interval time.Duration
	store    *Store
	onEvent  func(Event)
}

// NewWatcher 创建仓库事件监听器。
func NewWatcher(repoPath string, interval time.Duration, store *Store, cb func(Event)) *Watcher {
	return &Watcher{repo: repoPath, interval: interval, store: store, onEvent: cb}
}

// Run 周期性轮询直到 ctx 取消。
func (w *Watcher) Run(ctx context.Context) {
	if _, err := w.PollOnce(); err != nil {
		fmt.Printf("gitwatch: initial poll error: %v\n", err)
	}
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events, err := w.PollOnce()
			if err != nil {
				fmt.Printf("gitwatch: poll error: %v\n", err)
				continue
			}
			for _, e := range events {
				w.onEvent(e)
			}
		}
	}
}

// gitRef 是一次 for-each-ref 的解析结果。
type gitRef struct {
	sha     string
	refname string
}

// listRefs 查询本地仓库的分支与标签。
func listRefs(repo string) ([]gitRef, error) {
	cmd := exec.Command("git", "-C", repo, "for-each-ref",
		"--format=%(objectname) %(*objectname) %(refname)", "refs/heads", "refs/tags")
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git for-each-ref: %w: %s", err, strings.TrimSpace(errBuf.String()))
	}
	var refs []gitRef
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		sha := fields[0]
		refname := fields[len(fields)-1]
		// 带注释标签：*objectname 为解引用的提交。
		if len(fields) == 3 && fields[1] != "" {
			sha = fields[1]
		}
		refs = append(refs, gitRef{sha: sha, refname: refname})
	}
	return refs, nil
}

// primedKey 是存储中的基线标记键。
const primedKey = ":primed:"

// PollOnce 轮询一次：
//   - 首轮轮询只建立基线，不为已存在的引用补发事件；
//   - 之后新出现的引用（新分支/新标签）或指向变化的引用产生事件。
func (w *Watcher) PollOnce() ([]Event, error) {
	refs, err := listRefs(w.repo)
	if err != nil {
		return nil, err
	}
	_, primed := w.store.GetRef(w.repo, primedKey)
	var events []Event
	for _, ref := range refs {
		previous, seen := w.store.GetRef(w.repo, ref.refname)
		if err := w.store.SetRef(w.repo, ref.refname, ref.sha); err != nil {
			return events, err
		}
		if !primed {
			continue // 首轮：仅建立基线
		}
		if seen && previous == ref.sha {
			continue // 无变化
		}
		var t EventType
		var name string
		switch {
		case strings.HasPrefix(ref.refname, "refs/heads/"):
			t = EventPush
			name = strings.TrimPrefix(ref.refname, "refs/heads/")
		case strings.HasPrefix(ref.refname, "refs/tags/"):
			t = EventTag
			name = strings.TrimPrefix(ref.refname, "refs/tags/")
		default:
			continue
		}
		e := Event{
			ID:         string(t) + ":" + name + "@" + ref.sha,
			Type:       t,
			Repo:       w.repo,
			Ref:        name,
			Commit:     ref.sha,
			ReceivedAt: time.Now(),
		}
		events = append(events, e)
	}
	if !primed {
		if err := w.store.SetRef(w.repo, primedKey, "1"); err != nil {
			return events, err
		}
	}
	return events, nil
}
