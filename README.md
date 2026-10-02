# 本地持续交付流水线服务（ci-cd-pipeline-go）

一个运行在本机、从**本地 Git 仓库**的提交/标签事件触发的持续交付流水线守护进程。
纯本地、零外部依赖（只需系统 `git`），用 JSON 文件持久化运行状态与产物。

- 提交 / 标签事件触发，分支与标签 glob 过滤
- 同一事件**绝不重复创建运行**（先占事件去重记录，再创建运行）
- 阶段**串行 / 并行**依赖（`group` 并行分组或显式 `needs` DAG）
- 七种运行状态：`pending` / `waiting` / `running` / `success` / `failed` / `canceled` / `skipped`
- 人工**审批门禁**：等待、可配置超时取消、拒绝必须填写可审计意见
- 失败**自动重试**与**手动重跑**：默认只重跑失败及受影响下游，成功阶段不重复执行；`force` 才全量重跑
- 运行/阶段**超时**、用户**取消**、**并发上限**；取消向子进程**整组传播**，不遗留孙子进程
- 运行状态与任务**产物**持久化；进程崩溃重启后按策略 `resume / cancel / fail` 恢复，不丢状态、不重复执行
- 本地 HTTP 控制接口 + `pipelinectl` 命令行

## 快速开始

```bash
go build ./...

# 1) 准备一个本地 git 仓库和一份配置
cp pipeline.example.json pipeline.json
# 修改 repo 为你的本地仓库绝对路径

# 2) 启动守护进程（默认轮询 2s，监听 127.0.0.1:8080，数据目录 .pipelined）
go run ./cmd/pipelined -config pipeline.json -data .pipelined

# 3) 在被监控仓库中产生提交/标签
cd /path/to/repo && git commit --allow-empty -m "trigger" && git tag v1.0.0

# 4) 用 CLI 查看与操作
go run ./cmd/pipelinectl list
go run ./cmd/pipelinectl get <run-id>
go run ./cmd/pipelinectl approve <run-id> -stage deploy -as alice
go run ./cmd/pipelinectl reject  <run-id> -stage deploy -as alice -comment "缺少验收记录"
go run ./cmd/pipelinectl cancel  <run-id> -reason "紧急中止"
go run ./cmd/pipelinectl rerun  <run-id>          # 只重跑失败/受影响阶段
go run ./cmd/pipelinectl rerun  <run-id> -force   # 全量重跑
```

守护进程参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-config` | `pipeline.json` | 流水线配置文件 |
| `-data` | `.pipelined` | 状态与产物目录 |
| `-addr` | `127.0.0.1:8080` | 控制 API 地址（仅本机） |
| `-recovery` | `resume` | 重启恢复策略：`resume` / `cancel` / `fail` |
| `-interval` | `2s` | Git 轮询间隔 |

## 配置语法

配置为 JSON（时长字段接受 `"30m"`、`"1h"`、`"90s"` 等字符串）。

```json
{
  "name": "example",
  "repo": "/abs/path/to/repo",
  "concurrency": 2,
  "run_timeout": "30m",
  "triggers": { "branches": ["main", "release/*"], "tags": ["v*"] },
  "stages": [ /* Stage[] */ ]
}
```

顶层字段：

- `name`（必填）：流水线名称。
- `repo`（必填）：被监控的本地 Git 工作树绝对路径；任务默认在此目录执行。
- `triggers`（至少一个匹配规则）：
  - `branches`：分支名 glob 列表，提交（push）事件命中其一才触发；为空则提交事件不触发。
  - `tags`：标签名 glob 列表，标签事件命中其一才触发。
- `concurrency`：同时运行的流水线上限，`0` 表示不限；超出的运行保持 `pending` 排队。
- `run_timeout`：单次运行总超时，超时后运行与所有子任务被取消。
- `stages`（至少一个阶段）。

### Stage

| 字段 | 说明 |
| --- | --- |
| `name` | 唯一阶段名（必填）。 |
| `group` | 并行分组：**同名 `group` 的阶段并行**；不同组按声明顺序串行。未设置时该阶段自成一组（串行）。 |
| `needs` | 显式依赖的阶段名数组（DAG）。一旦设置，以 `needs` 为准；未设置时按 `group` 顺序自动推导依赖。禁止环依赖。 |
| `approval` | 审批门禁，见下。 |
| `timeout` | 阶段级超时；超时取消该阶段。 |
| `retry` | 阶段内任务失败后的**自动重试次数**（`retry:2` 表示最多执行 3 次）。取消不重试。 |
| `tasks` | 阶段内任务，**串行**执行。 |

### Task

| 字段 | 说明 |
| --- | --- |
| `name` | 任务名（必填）。 |
| `command` / `args` | 直接执行的程序及参数（`shell:false`）。 |
| `shell` | 为 `true` 时以 `sh -c "<command>"` 执行。 |
| `workdir` | 任务工作目录；相对路径相对于 `repo`。 |
| `env` | 形如 `KEY=VALUE` 的额外环境变量。 |
| `allow_failure` | 为 `true` 时任务失败不导致阶段失败。 |
| `artifacts` | 成功后收集的 glob（相对工作目录），复制到 `<data>/artifacts/<run>/<stage>/<task>/`，并把相对路径记入任务状态。 |

### Approval

```json
"approval": { "required": true, "approvers": ["alice"], "timeout": "2h" }
```

- `required`：是否在阶段执行前等待人工审批。
- `approvers`：允许审批的人；为空表示任何人都可审批。
- `timeout`：等待审批的最长时间；超时后审批与阶段被**取消**。
- 拒绝时**必须提供 comment**，审批人、意见、时间全部持久化在运行记录中，可追溯。

## 状态语义

| 状态 | Run / Stage / Task 含义 |
| --- | --- |
| `pending` | 已创建但尚未开始：排队等并发槽，或尚未被调度评估。 |
| `waiting` | **等待中**：阶段在等上游阶段完成，或停在人工审批门前；运行在所有未决阶段都阻塞（无任务在执行）时呈现等待。 |
| `running` | 正在执行（运行至少有一个阶段在执行任务）。 |
| `success` | 成功完成（运行要求所有阶段成功）。 |
| `failed` | 任务失败（重试耗尽/审批被拒），或整体存在失败/被跳过阶段。 |
| `canceled` | 被用户取消、运行/阶段/审批超时、或守护进程关闭；正在执行的子进程被整组 SIGKILL。 |
| `skipped` | 上游阶段 `failed/canceled`，下游不再启动，标记为跳过。 |

### 等待状态（waiting）

`waiting` 是新增的非终态取值，用于把「等待」与「未开始（`pending`）」「执行中（`running`）」明确区分：

- **阶段等待上游**：调度器判定某阶段的上游尚未全部成功时，该阶段从 `pending` 变为 `waiting`，
  直到上游就绪后才进入 `running`。
- **阶段等待审批**：阶段到达人工审批门、审批记录处于 `pending` 时，阶段呈现 `waiting`。
- **运行等待**：当运行没有任何执行中的阶段、且仍有阶段处于 `waiting`（等审批或等上游）时，
  运行整体呈现 `waiting`；一旦有阶段开始执行即回到 `running`。

等待状态会随运行状态**一并持久化**，因此 `pipelinectl get`、HTTP 查询与重启后的恢复流程看到的
等待语义一致；恢复流程不会把等待中的运行误判为执行中（等待的阶段不会被当作崩溃中断而重跑）。

判定规则：

- 任一上游未成功 → 下游**不会启动**；上游失败时下游记 `skipped`。
- 运行结果：有失败/跳过阶段 ⇒ `failed`；否则有取消阶段 ⇒ `canceled`；全部成功 ⇒ `success`。

## 事件去重

### 启动基线

守护进程**启动后的首轮仓库扫描只建立事件基线，不产生任何运行**：扫描到的、在启动之前就
已存在的提交与标签会被记录为「已见」，但不会触发流水线。只有**启动之后新产生**的提交或
标签才会触发运行。因此对一个已有历史提交与标签的仓库启动服务，首轮扫描的事件数为零、
不会为重放的历史事件创建任何运行。

### 去重规则

事件的稳定 ID 为 `<kind>:<ref>:<commit>`（如 `push:main:<sha>`、`tag:v1:<sha>`）。
分支头未变化时反复扫描不会重复触发；强推产生新 SHA、或新标签，才会产生新事件。
处理顺序为「**先原子创建事件记录（O_EXCL）→ 再创建运行**」，因此即使事件并发到达或进程崩溃，
同一事件最多产生一个运行，且不会留下幽灵运行文件。
事件记录持久化在 `<data>/events/` 下，**进程重启后仍然有效**：重复扫描或重启后重复到达的
同一事件会被识别并拒绝，不会重复创建运行。

## 重试与重跑

- **自动重试**：仅针对失败的任务，按阶段 `retry` 配置进行；成功任务不重跑；取消/超时不重试。
- **手动重跑（rerun）**：把所有非成功阶段重置为 `pending`，并把**传递依赖到这些阶段的下游一并重置**
  （即使它们曾经成功，也不能信任）；无依赖关系的成功阶段保持不变、绝不重复执行。
- **强制全量重跑（rerun -force）**：显式要求时，所有阶段都重新执行。

## 取消、超时与并发

- 用户取消、运行/阶段超时、守护进程收到 SIGINT/SIGTERM 都会取消对应 context。
- 每个任务在独立**进程组**中启动；取消时向负 PID 发 `SIGKILL`，`sh` 派生的孙子进程一并被回收，
  引擎跟踪活动子进程并在退出前校验 `active child processes at exit: 0`。
- `concurrency` 限制并发运行数，超额运行保持 `pending` 排队，槽位释放后自动开始。

### 取消与启动并发的时序保证

取消与运行启动并发时行为是**确定**的，与具体交错时序无关：

- 取消请求一旦被接受（运行尚未到达终态），运行**必然在有限时间内收敛到 `canceled`**，
  不会停留在 `running`/`waiting` 无法收尾，也不会再启动新的阶段或任务。
- 正在执行的子任务会收到取消信号并以 `SIGKILL` 整组退出，不会遗留继续运行的子进程。
- 运行已到达终态后再取消会被拒绝（`run is not active`）；**先到者生效**——若取消先于运行
  收尾落盘，则运行记为 `canceled`，否则保持已完成的终态。
- 取消发生在**等待审批**的运行上时同样立即生效：审批记录被标记为 `canceled` 并**保留取消
  原因**，等待中的下游阶段不会启动。

## 持久化与崩溃恢复

数据目录布局：

```
<data>/runs/<run-id>.json      # 每次状态变更都原子写入（temp + rename）
<data>/events/<event-id>.json  # 事件去重记录（O_EXCL 创建）
<data>/artifacts/<run>/<stage>/<task>/...  # 收集到的产物
```

启动时 `Recover(policy)` 扫描所有非终态运行：

- `resume`（默认）：磁盘上处于 `running` 的阶段只能是崩溃中断，重置该阶段为 `pending` 并把其
  传递下游一并重置（含曾经成功但依赖被中断阶段的阶段）；已经 `success` 的阶段保留不重跑；
  已存在的 pending 审批会重新挂回内存，可继续审批。
  处于 `waiting` 的阶段（等上游或等审批）**不是**崩溃中断，保持等待不被重跑；等待中的运行
  恢复后仍回到 `waiting`，审批语义与重启前一致。
- `cancel`：所有未完成运行标记为 `canceled`。
- `fail`：所有未完成运行标记为 `failed`。
- 终态运行一律不动，杜绝重启后重复执行。

## HTTP 控制接口

```
GET  /runs
GET  /runs/{id}
POST /runs/{id}/cancel   {"reason":"..."}
POST /runs/{id}/rerun    {"force":false}
POST /runs/{id}/approve  {"stage":"deploy","approver":"alice"}
POST /runs/{id}/reject   {"stage":"deploy","approver":"alice","comment":"..."}
```

## 架构

```
cmd/pipelined       守护进程：加载配置、打开存储、恢复、git watcher、HTTP 服务
cmd/pipelinectl     CLI 客户端
internal/git        轮询本地 git（for-each-ref / rev-list / show），生成去重事件
internal/event      分支/标签 glob 过滤 + 事件去重
internal/config     配置加载与校验（唯一阶段名、needs 引用、DAG 环检测）
internal/store      JSON 文件存储（原子写、事件 O_EXCL 去重）
internal/executor   子进程执行、进程组 SIGKILL、活动进程跟踪、输出截断
internal/approval   审批门禁（等待/通过/拒绝/超时，决策可审计）
internal/artifacts  任务产物收集
internal/engine      编排核心：调度器（DAG/并行组）、重试、取消、并发槽、超时、恢复
internal/server     本地 HTTP 控制面
```

## 测试

```bash
go test ./... -race -count=1
```

本地验证（在竞态检测下连续多轮，确认无不稳定失败）：

```bash
# 全量测试 + 竞态检测
go test ./... -race -count=1

# 关键并发/恢复场景连续跑 10 轮，必须全部稳定通过
go test ./internal/engine/ -race -count=10 \
  -run 'TestCancelImmediatelyAfterCreateAlwaysConverges|TestCancelDuringApprovalLeavesNoChildren|TestWaitingStatusSurvivesRestart|TestFirstScanBaselineProducesNoRuns'
```

覆盖场景：重复事件去重、串行/并行阶段依赖与下游跳过、审批通过/拒绝（意见可追溯）/超时、
自动重试、手动重跑（成功阶段不重复 + force 全量）、运行取消（子进程不遗留）、并发上限排队、
运行/阶段超时、`resume/cancel/fail` 重启恢复、产物收集，以及基于真实临时 git 仓库与真实
HTTP 服务的提交→触发→审批→成功端到端验收。

针对本次三类问题新增的覆盖：

- **启动基线**：在含历史提交与标签的真实仓库上启动，断言首轮扫描事件数为零、不产生任何运行
  （`TestWatcherFirstScanIsBaseline`、`TestFirstScanBaselineProducesNoRuns`）。
- **重复事件**：同一事件重复触发、重复扫描、进程重启后重复到达均不产生重复运行
  （`TestDuplicateEventCreatesOneRun`、`TestDuplicateEventAfterRestartNoNewRun`）。
- **等待状态**：等待上游/等待审批在查询与持久化中呈现 `waiting`，重启恢复后等待语义不变
  （`TestWaitingStatusVisibleAndPersisted`、`TestWaitingOnUpstreamStageVisible`、`TestWaitingStatusSurvivesRestart`）。
- **取消并发**：运行创建后立即取消的时序交错下取消必然收敛、不遗留子进程；等待审批时取消
  保留取消原因、下游不启动、不遗留子进程
  （`TestCancelImmediatelyAfterCreateAlwaysConverges`、`TestCancelDuringApprovalLeavesNoChildren`、
  `TestMarkUnfinishedConvergesWhenStagesTerminal`）。
