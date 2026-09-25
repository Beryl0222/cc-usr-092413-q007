# 颅内决策实验编排

服务用于协调临床优先条件下的颅内电极研究，保存刺激、行为与脑信号的时间关系和安全决定。

项目当前提供稳定的基础服务入口，便于本地联调和运维巡检。运行 `python3 service.py --check` 可核对服务配置；执行 `python3 service.py --port 8000` 后访问 `/health` 可确认服务身份。

## 临床中止数据解封裁决

当参与者出现不良反应、临床团队中止后续环节时，服务按以下规则管理已采集片段：

1. **封存**：`POST /suspension-events` 按统一时间轴封存事件前后窗口内的片段，
   并随事件快照设备状态、医嘱与当时同意范围。已封存片段默认隔离，
   任何分析任务（`POST /analysis-tasks`）只读到隔离之外的数据。
2. **两阶段裁决**：临床安全人员先判定可能受治疗或异常状态影响的区间
   （`POST /suspension-events/{id}/safety-assessment`），研究方法人员再判定
   预注册终点是否仍可解释（`POST .../methodology-assessment`）。
3. **最小范围处置**：两阶段齐备后，数据管理员对全部封存片段逐一签署
   `unseal`（解封）、`exclude_permanently`（永久排除）或
   `safety_review_only`（仅安全复盘）（`POST .../dispositions`，
   需携带 `expected_version` 做乐观并发控制）。
4. **时钟校正**：`POST /clock-corrections` 只提出影响区间并开启修订；
   已裁决版本不被静默改写，修订需重新走完整两阶段流程后落为新版本。
5. **参与者撤回**：`POST /participants/{id}/withdrawal` 停止新的研究使用，
   依法必须保存的安全记录仍可通过 `purpose=safety_review` 复盘。
6. **并发与幂等**：所有状态迁移在单把锁内完成，并发裁决只产生一个当前版本；
   同内容重试（或携带同一 `request_id`）安全回放；分歧不覆盖既有结论，
   转入伦理复核（`GET /ethics-reviews`，`POST /ethics-reviews/{id}/resolve`）。
7. **重启恢复**：以 `--data-dir <目录>` 启动后状态持久化到
   `<目录>/state.json`，重启后 `GET /adjudication-queue` 列出待裁决工作。
8. **审计追溯**：`GET /audit/analysis-results/{id}` 从任一分析结果追到
   中止事件、区间选择、签署决定和后来修订；
   `GET /audit/suspension-events/{id}` 提供事件视角的完整版本史。

### 角色

除 `/health` 外，所有接口要求请求头 `X-Actor-Role`（写操作另需 `X-Actor-Id`
以签署决定）：

| 角色 | 职责 |
| --- | --- |
| `clinical_safety` | 登记中止事件、临床安全评估、时钟校正、安全复盘 |
| `research_methodology` | 研究方法评估 |
| `data_administrator` | 最小范围处置（解封/排除/仅复盘） |
| `ethics_coordinator` | 撤回登记、伦理复核结论 |
| `device_engineer` | 时钟校正提议 |
| `research_staff` | 参与者与片段登记、研究分析 |

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
