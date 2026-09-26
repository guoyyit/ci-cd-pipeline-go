package pipeline

import "time"

// Status 表示运行、阶段或任务的状态。
type Status string

const (
	StatusPending   Status = "pending"   // 等待调度
	StatusWaiting   Status = "waiting"   // 等待人工审批
	StatusRunning   Status = "running"   // 执行中
	StatusSuccess   Status = "success"   // 成功
	StatusFailed    Status = "failed"    // 失败
	StatusCancelled Status = "cancelled" // 已取消
	StatusSkipped   Status = "skipped"   // 已跳过
)

// Terminal 报告状态是否为终态。
func (s Status) Terminal() bool {
	switch s {
	case StatusSuccess, StatusFailed, StatusCancelled, StatusSkipped:
		return true
	}
	return false
}

// Valid 报告状态是否为合法取值。
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusWaiting, StatusRunning, StatusSuccess,
		StatusFailed, StatusCancelled, StatusSkipped:
		return true
	}
	return false
}

// EventType 是触发事件类型。
type EventType string

const (
	EventPush EventType = "push"
	EventTag  EventType = "tag"
)

// Event 是来自本地 Git 仓库的一次提交或标签事件。
type Event struct {
	ID         string    `json:"id"`
	Type       EventType `json:"type"`
	Repo       string    `json:"repo"`
	Ref        string    `json:"ref"`
	Commit     string    `json:"commit"`
	ReceivedAt time.Time `json:"received_at"`
}

// Key 返回事件去重键。
func (e Event) Key() string {
	return e.Repo + ":" + string(e.Type) + ":" + e.Ref + ":" + e.Commit
}

// ApprovalState 记录人工审批门禁状态。
type ApprovalState struct {
	Required  bool       `json:"required"`
	Decided   bool       `json:"decided"`
	Approved  bool       `json:"approved"`
	Approver  string     `json:"approver,omitempty"`
	Reason    string     `json:"reason,omitempty"`
	DecidedAt *time.Time `json:"decided_at,omitempty"`
	Deadline  *time.Time `json:"deadline,omitempty"`
}

// TaskRun 是单个任务的执行记录。
type TaskRun struct {
	Name      string     `json:"name"`
	Status    Status     `json:"status"`
	ExitCode  int        `json:"exit_code"`
	LogPath   string     `json:"log_path,omitempty"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	Attempts  int        `json:"attempts"`
	Error     string     `json:"error,omitempty"`
}

// StageRun 是单个阶段的执行记录。
type StageRun struct {
	Name      string         `json:"name"`
	Status    Status         `json:"status"`
	Approval  *ApprovalState `json:"approval,omitempty"`
	Tasks     []*TaskRun     `json:"tasks"`
	Attempts  int            `json:"attempts"`
	StartedAt *time.Time     `json:"started_at,omitempty"`
	EndedAt   *time.Time     `json:"ended_at,omitempty"`
	Error     string         `json:"error,omitempty"`
}

// Run 是一次流水线运行。
type Run struct {
	ID         string               `json:"id"`
	Pipeline   string               `json:"pipeline"`
	Event      Event                `json:"event"`
	Status     Status               `json:"status"`
	Stages     map[string]*StageRun `json:"stages"`
	StageOrder []string             `json:"stage_order"`
	Attempt    int                  `json:"attempt"`
	CreatedAt  time.Time            `json:"created_at"`
	StartedAt  *time.Time           `json:"started_at,omitempty"`
	EndedAt    *time.Time           `json:"ended_at,omitempty"`
	Error      string               `json:"error,omitempty"`
}
