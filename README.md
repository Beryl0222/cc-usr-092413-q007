# 颅内决策实验编排

服务用于协调临床优先条件下的颅内电极研究，保存刺激、行为与脑信号的时间关系和安全决定。

当参与者在实验中途出现不良反应时，服务提供**中止后的数据隔离与解封裁决**：安全事件按统一时间轴封存前后片段、设备状态、医嘱与当时同意范围；任何分析任务默认只能读到隔离之外的数据；经临床安全与方法学双阶段裁决（分歧时伦理复核）后，数据管理员才能按最小范围解封、永久排除或仅允许安全复盘。

## 运行

```bash
python3 service.py --check          # 核对服务配置与存储自检
python3 service.py --port 8000      # 启动服务（默认库 adjudication.db，--db 指定路径）
curl localhost:8000/health          # 确认服务身份
```

## 领域规则

### 封存即隔离

`POST /safety-events` 在统一时间轴上记录中止时刻，把该会话的全部片段（按 `pre`/`at`/`post` 标注）、设备状态、医嘱与当时同意范围冻结为不可变快照。被封存片段即刻进入**默认隔离**：分析任务的可读集由服务端裁定，隔离中的片段一律不可读。

### 双阶段裁决与伦理复核

每个裁决轮按固定顺序推进（`review_rounds.stage`）：

```
await_clinical → await_methodology → await_application → complete
                                      ↑ 分歧时 await_ethics ┘
```

1. **临床安全**（`clinical_safety`）逐片段判断是否可能受治疗或异常状态影响（`affected`）；
2. **方法学**（`methodology`）逐片段判断预注册终点是否仍可解释（`interpretable`）；
3. 两阶段结论一致时直接得出决定：未受影响且可解释 → `release`；受影响且不可解释 → `exclude`；**结论分歧的片段进入伦理复核**（`ethics`），伦理可裁 `release` / `exclude` / `safety_only`；
4. 两种结论（分歧时含伦理）都完成后，**数据管理员** `POST /safety-events/{id}/apply` 才能应用，生成一个裁决版本。

评审必须恰好覆盖本轮影响区间内的全部片段，且须带签署（`reviewer` + `signature`）。

### 版本化与迟到的时钟校正

- 每次应用产生一个 `decision_versions` 版本；**任一时刻每个事件至多一个当前版本**（数据库部分唯一索引保证，并发冲突返回 409）。
- 迟到的时钟校正（`POST /safety-events/{id}/clock-corrections`）只能提出 `delta` 与**影响区间**；被接受后开启新一轮裁决，影响区间内未封存的片段立即补封进入默认隔离。新版本应用后旧版本被标记取代（`superseded_by`），**旧版本内容永不改写**；区间外片段的决定沿用上一版本。
- 校正不能在裁决轮进行中接受（409 `adjudication_in_progress`）。

### 参与者撤回

`POST /participants/{id}/withdraw` 后：不能开新会话；显式针对该参与者的研究任务被拒绝（409 `participant_withdrawn`）；已撤回参与者的数据不进入任何新研究任务的可读集与结果登记。**依法必须保存的安全记录保留**：安全复盘任务（`kind=safety_review`）仍可读取 `release` 与 `safety_only` 片段，封存快照与审计链完整保留。

### 分析任务与血缘

- `POST /analysis-tasks`（`kind=research|safety_review`）在创建时冻结可读集快照；研究任务必须指明预注册终点 `endpoint_id`。
- `POST /analysis-results` 登记结果时校验每个输入片段都在任务可读集内，并把片段当时依据的裁决版本写入血缘。
- `GET /analysis-results/{id}/trace` 从任一分析结果追到：中止事件、各轮影响区间选择、三方签署评审、放行版本与后来的修订、时钟校正。

### 幂等与并发

- 所有写接口支持 `Idempotency-Key` 请求头：同键重放返回首次响应（`Idempotent-Replay: true`），同键不同请求体/路径返回 409。
- 领域层同内容重试安全：重复登记、重复提交相同评审、重复应用均幂等返回既有结果；同轮次同角色提交**不同**内容的评审返回 409 `review_conflict`（分歧走伦理复核，不允许覆盖）。
- 写操作在单事务内串行化（`BEGIN IMMEDIATE`），审计与领域变更同事务落库。

### 持久化与审计

全部状态存于 SQLite（WAL），**服务重启后待裁决工作继续**：`GET /safety-events/pending` 列出未完成的裁决轮与待决定的时钟校正。审计日志为只增哈希链（`GET /audit/verify` 校验完整性，`GET /audit/{entity_type}/{entity_id}` 查询实体轨迹）。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 服务身份 |
| POST | `/participants` | 登记参与者与同意范围 |
| POST | `/participants/{id}/withdraw` | 参与者撤回 |
| POST | `/sessions` | 开启会话 |
| POST | `/segments` | 登记刺激/行为/脑信号片段 |
| POST | `/device-states` | 登记设备状态 |
| POST | `/medical-orders` | 登记医嘱 |
| POST | `/safety-events` | 宣告安全事件并封存 |
| GET | `/safety-events/{id}` | 事件全貌（封存、轮次、评审、版本、校正） |
| GET | `/safety-events/pending` | 待裁决工作 |
| POST | `/safety-events/{id}/reviews` | 提交签署评审（临床/方法学/伦理） |
| POST | `/safety-events/{id}/apply` | 数据管理员应用裁决版本 |
| POST | `/safety-events/{id}/clock-corrections` | 提出时钟校正（影响区间） |
| POST | `/clock-corrections/{id}/decision` | 接受/拒绝校正 |
| POST | `/analysis-tasks` | 创建分析任务（服务端裁定可读集） |
| POST | `/analysis-results` | 登记分析结果（校验可读集） |
| GET | `/analysis-results/{id}/trace` | 结果血缘追溯 |
| GET | `/audit/{entity_type}/{entity_id}` | 实体审计轨迹 |
| GET | `/audit/verify` | 审计链完整性校验 |

## 测试与构建

执行完整测试：

```bash
npm test
```

执行编译检查：

```bash
python3 -m compileall -q .
```

两条命令都可在单个 Linux 应用容器内直接运行，不需要额外服务。
