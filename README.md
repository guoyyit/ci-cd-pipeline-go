# 本地持续交付流水线服务 (ci-cd-pipeline-go)

一个单机运行的轻量持续交付服务：监听**本地 Git 仓库**的提交与标签事件，按声明式
JSON 配置编排串行/并行阶段（Stage），每个阶段包含若干本机执行的任务（Task），
支持人工审批、失败重试、取消传播、并发上限、运行/任务超时、产物收集与崩溃恢复。

## 快速开始

```bash
go build -o pipelined ./cmd/pipelined

# 1) 准备一个本地 git 仓库
git init -b main /tmp/myrepo && cd /tmp/myrepo && echo hi > f && git add . && git commit -m init

# 2) 编写 pipeline.json（参考 pipeline.example.json，repo.path 指向 /tmp/myrepo）
# 3) 启动服务
./pipelined -config pipeline.json -addr :8080 -poll 2s

# 4) 在仓库产生提交或标签，服务自动触发流水线
cd /tmp/myrepo && echo change >> f && git commit -am change
git tag v1.0.0

# 5) 查询运行
curl localhost:8080/api/runs
```

## 配置语法

配置文件为 JSON（见 `pipeline.example.json`）：

| 字段 | 说明 |
| --- | --- |
| `name` | 流水线名称，必填 |
| `repo.path` | 被监听的本地 Git 仓库路径；任务在此目录内执行 |
| `trigger.branches` | 分支 glob 白名单（如 `["main","release/*"]`），匹配的分支提交才触发 |
| `trigger.tags` | 标签 glob 白名单（如 `["v*"]`），匹配的标签才触发 |
| `settings.data_dir` | 状态、日志、产物目录，默认 `./data` |
| `settings.max_concurrent_runs` | 全局并发运行上限，默认 2，超出排队 |
| `settings.run_timeout` | 单次运行整体超时（如 `"30m"`），默认 30 分钟 |
| `stages` | 阶段数组，至少一个 |

阶段字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 阶段名，必须唯一 |
| `needs` | 依赖的阶段名数组；**无依赖的阶段并行执行，有依赖的阶段在依赖全部成功后执行**；依赖图不允许成环 |
| `approval.timeout` | 设置后该阶段为人工审批门禁，进入前保持 `waiting`；超时（如 `"24h"`）自动取消 |
| `retries` | 阶段内任务失败后的自动重试次数（只重跑失败任务），默认 0 |
| `timeout` | 阶段超时 |
| `tasks` | 阶段内任务数组，**顺序执行**，任一失败即终止该阶段 |

任务字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 任务名（阶段内唯一） |
| `run` | 经 `sh -c` 在仓库目录执行的命令，必填 |
| `env` | 附加环境变量（覆盖同名继承变量） |
| `timeout` | 任务超时（如 `"5m"`） |
| `artifacts` | 相对仓库目录的文件 glob，执行后收集到 `data_dir/runs/<run>/artifacts/` |

时间间隔字段接受 Go duration 字符串：`"30s"`、`"5m"`、`"24h"`。

## 状态语义

运行/阶段/任务使用统一状态机：

| 状态 | 含义 |
| --- | --- |
| `pending` 等待 | 已创建但尚未开始（排队中，或等待前序阶段） |
| `waiting` 执行等待 | 阶段已具备执行条件，但被**人工审批门禁**挂起 |
| `running` 执行中 | 阶段/任务正在执行 |
| `success` 成功 | 全部任务退出码为 0 |
| `failed` 失败 | 命令非零退出、超时，或崩溃中断（`interrupted: service restarted`） |
| `cancelled` 取消 | 被手动取消、运行超时、审批超时或审批拒绝 |
| `skipped` 跳过 | 前序依赖失败/取消/跳过时，下游阶段自动跳过 |

状态传播规则：

- 前序阶段未成功（失败/取消/跳过）→ 下游不会启动，标记 `skipped`。
- 阶段内任务顺序执行，前一个失败则后续任务跳过；阶段标记 `failed`。
- 审批通过后阶段从 `waiting` 回到 `pending` 并立即调度；审批拒绝记录
  审批人、理由、时间戳，阶段为 `cancelled`，下游 `skipped`。
- 运行终态由阶段聚合：有失败→`failed`；无失败有取消→`cancelled`；全部成功/跳过→`success`。

## 事件去重

事件键为 `repo:type:ref:commit`。同一分支/标签指向同一提交的重复事件
（监听器重复轮询、重启后重放、HTTP 重复投递）**只创建一次运行**，
重复请求返回已存在的运行。事件记录持久化在状态文件中。

## 重试与重跑

- **自动重试**：阶段配置 `retries`，任务失败后只重跑该阶段内失败的任务。
- **手动重试**：`POST /api/runs/{id}/retry` 只重置失败/取消阶段及其下游
  受影响阶段；**成功阶段保持不执行**。
- **强制全量重跑**：`POST /api/runs/{id}/retry?force=1` 重置全部阶段
  （包括成功阶段），运行 attempt 加 1。

## 取消与并发

- 取消（手动或运行超时）通过 `context` 传播到正在执行的命令；任务在独立
  **进程组**中运行，取消时向整个进程组发送 `SIGKILL`（含 5s `WaitDelay`），
  不遗留子进程。
- `max_concurrent_runs` 限制同时执行的运行数，超出运行保持 `pending` 排队。

## 持久化与崩溃恢复

- 全部运行、事件与引用基线原子写入 `data_dir/state.json`
  （临时文件 + rename，不会写坏）；每次状态变更立即落盘。
- 任务日志：`data_dir/runs/<run>/logs/<stage>/<task>.log`。
- 产物：`data_dir/runs/<run>/artifacts/<stage>/<task>/`。
- 重启后对未完成运行执行恢复：崩溃时处于 `running` 的阶段/任务标记为
  `failed`（原因 `interrupted: service restarted`），可手动重试；
  `pending`/`waiting` 阶段继续调度；已成功阶段**绝不重复执行**。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/runs` | 列出运行（按创建时间升序） |
| GET | `/api/runs/{id}` | 运行详情（含阶段/任务状态、审批记录） |
| POST | `/api/runs/{id}/cancel` | 取消运行 |
| POST | `/api/runs/{id}/retry?force=1` | 重试（默认仅失败及下游，`force=1` 全量） |
| POST | `/api/runs/{id}/stages/{stage}/approve` | 审批通过，body：`{"approver":"alice"}` |
| POST | `/api/runs/{id}/stages/{stage}/reject` | 审批拒绝，body：`{"approver":"bob","reason":"..."}`（reason 必填） |
| POST | `/api/events` | 手动投递事件 `{"type":"push|tag","repo":"...","ref":"main","commit":"..."}` |
| GET | `/healthz` | 健康检查 |

## 测试

```bash
go test ./... -race
```

覆盖场景：重复事件去重、分支/标签过滤、阶段串行/并行依赖与下游跳过、
人工审批（通过/拒绝可追溯/超时）、失败重试与强制全量重跑、取消传播
（校验子进程被杀死）、审批门禁、Git 监听器（真实仓库的提交/标签检测与
重启基线恢复）、持久化与崩溃恢复，以及真实 Git 仓库 + HTTP API 的端到端流程。

## 项目结构

```
cmd/pipelined/            服务入口（配置加载、信号优雅退出）
internal/pipeline/
  types.go                状态、事件、运行等领域模型
  config.go               配置加载/校验/DAG 校验/glob 过滤
  store.go                JSON 原子持久化
  executor.go             本机任务执行（进程组、超时、日志、产物）
  scheduler.go            DAG 状态机（调度/跳过/重试压平/状态聚合）
  runner.go               编排器（事件循环、审批、并发、恢复）
  gitwatch.go             本地 Git 引用轮询
  server.go               HTTP API
```
