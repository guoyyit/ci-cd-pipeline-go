package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"time"
)

// Duration 是支持 JSON 字符串（如 "10m"）的时间间隔。
type Duration struct {
	time.Duration
}

// UnmarshalJSON 实现 json.Unmarshaler，支持 "10m" 字符串或纳秒数字。
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch t := v.(type) {
	case string:
		if t == "" {
			d.Duration = 0
			return nil
		}
		dur, err := time.ParseDuration(t)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", t, err)
		}
		d.Duration = dur
		return nil
	case float64:
		d.Duration = time.Duration(t)
		return nil
	case nil:
		d.Duration = 0
		return nil
	default:
		return fmt.Errorf("invalid duration value: %v", v)
	}
}

// MarshalJSON 实现 json.Marshaler。
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// RepoConfig 描述监听的本地仓库。
type RepoConfig struct {
	Path string `json:"path"`
}

// TriggerConfig 描述分支/标签过滤规则（glob）。
type TriggerConfig struct {
	Branches []string `json:"branches"`
	Tags     []string `json:"tags"`
}

// ApprovalConfig 描述人工审批门禁。
type ApprovalConfig struct {
	Timeout Duration `json:"timeout"`
}

// TaskConfig 描述阶段内的一个任务。
type TaskConfig struct {
	Name      string            `json:"name"`
	Run       string            `json:"run"`
	Env       map[string]string `json:"env,omitempty"`
	Timeout   Duration          `json:"timeout,omitempty"`
	Artifacts []string          `json:"artifacts,omitempty"`
}

// StageConfig 描述一个阶段。
type StageConfig struct {
	Name     string          `json:"name"`
	Needs    []string        `json:"needs,omitempty"`
	Approval *ApprovalConfig `json:"approval,omitempty"`
	Retries  int             `json:"retries,omitempty"`
	Timeout  Duration        `json:"timeout,omitempty"`
	Tasks    []TaskConfig    `json:"tasks"`
}

// Settings 是全局运行设置。
type Settings struct {
	DataDir           string   `json:"data_dir"`
	MaxConcurrentRuns int      `json:"max_concurrent_runs"`
	RunTimeout        Duration `json:"run_timeout"`
}

// Config 是流水线声明式配置。
type Config struct {
	Name     string        `json:"name"`
	Repo     RepoConfig    `json:"repo"`
	Trigger  TriggerConfig `json:"trigger"`
	Settings Settings      `json:"settings"`
	Stages   []StageConfig `json:"stages"`
}

// LoadConfig 从文件加载配置并应用默认值、执行校验。
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	c.setDefaults()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &c, nil
}

func (c *Config) setDefaults() {
	if c.Settings.DataDir == "" {
		c.Settings.DataDir = "data"
	}
	if c.Settings.MaxConcurrentRuns <= 0 {
		c.Settings.MaxConcurrentRuns = 2
	}
	if c.Settings.RunTimeout.Duration <= 0 {
		c.Settings.RunTimeout = Duration{30 * time.Minute}
	}
	for i := range c.Trigger.Branches {
		if c.Trigger.Branches[i] == "" {
			c.Trigger.Branches[i] = "*"
		}
	}
}

// Validate 校验配置合法性。
func (c *Config) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(c.Stages) == 0 {
		return fmt.Errorf("at least one stage is required")
	}
	names := map[string]bool{}
	for i := range c.Stages {
		st := &c.Stages[i]
		if st.Name == "" {
			return fmt.Errorf("stage #%d: name is required", i)
		}
		if names[st.Name] {
			return fmt.Errorf("duplicate stage name %q", st.Name)
		}
		names[st.Name] = true
		if len(st.Tasks) == 0 {
			return fmt.Errorf("stage %q: at least one task is required", st.Name)
		}
		taskNames := map[string]bool{}
		for j := range st.Tasks {
			t := &st.Tasks[j]
			if t.Name == "" {
				return fmt.Errorf("stage %q task #%d: name is required", st.Name, j)
			}
			if taskNames[t.Name] {
				return fmt.Errorf("stage %q: duplicate task name %q", st.Name, t.Name)
			}
			taskNames[t.Name] = true
			if t.Run == "" {
				return fmt.Errorf("stage %q task %q: run command is required", st.Name, t.Name)
			}
		}
	}
	for i := range c.Stages {
		for _, dep := range c.Stages[i].Needs {
			if !names[dep] {
				return fmt.Errorf("stage %q: unknown dependency %q", c.Stages[i].Name, dep)
			}
			if dep == c.Stages[i].Name {
				return fmt.Errorf("stage %q: cannot depend on itself", c.Stages[i].Name)
			}
		}
	}
	if err := c.checkAcyclic(); err != nil {
		return err
	}
	return nil
}

// checkAcyclic 检测阶段依赖图中的环。
func (c *Config) checkAcyclic() error {
	const (
		white = 0 // 未访问
		gray  = 1 // 访问中
		black = 2 // 已完成
	)
	color := map[string]int{}
	needs := map[string][]string{}
	for _, st := range c.Stages {
		needs[st.Name] = st.Needs
	}
	var visit func(n string) error
	visit = func(n string) error {
		switch color[n] {
		case gray:
			return fmt.Errorf("dependency cycle detected at stage %q", n)
		case black:
			return nil
		}
		color[n] = gray
		for _, dep := range needs[n] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		color[n] = black
		return nil
	}
	for _, st := range c.Stages {
		if err := visit(st.Name); err != nil {
			return err
		}
	}
	return nil
}

// Stage 按名称查找阶段配置。
func (c *Config) Stage(name string) *StageConfig {
	for i := range c.Stages {
		if c.Stages[i].Name == name {
			return &c.Stages[i]
		}
	}
	return nil
}

// StageNames 返回按配置顺序的阶段名。
func (c *Config) StageNames() []string {
	names := make([]string, 0, len(c.Stages))
	for _, st := range c.Stages {
		names = append(names, st.Name)
	}
	return names
}

// MatchAny 报告 ref 是否匹配任一 glob 规则；空规则列表不匹配任何引用。
func MatchAny(patterns []string, ref string) bool {
	for _, p := range patterns {
		if p == "" {
			continue
		}
		if p == ref {
			return true
		}
		if ok, err := path.Match(p, ref); err == nil && ok {
			return true
		}
	}
	return false
}

// TopoStages 返回拓扑排序后的阶段名（稳定序：按配置顺序）。
func (c *Config) TopoStages() []string {
	indeg := map[string]int{}
	children := map[string][]string{}
	for _, st := range c.Stages {
		indeg[st.Name] = len(st.Needs)
		for _, dep := range st.Needs {
			children[dep] = append(children[dep], st.Name)
		}
	}
	var ready []string
	for _, st := range c.Stages {
		if indeg[st.Name] == 0 {
			ready = append(ready, st.Name)
		}
	}
	var order []string
	for len(ready) > 0 {
		n := ready[0]
		ready = ready[1:]
		order = append(order, n)
		sort.Strings(children[n])
		for _, ch := range children[n] {
			indeg[ch]--
			if indeg[ch] == 0 {
				ready = append(ready, ch)
			}
		}
	}
	return order
}
