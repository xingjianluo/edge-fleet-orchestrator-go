# Edge Fleet Orchestrator（边缘设备舰队多集群编排与断网同步）

用**本地内存 + 本地文件**模拟「云端控制面 / 边缘网关 / 设备端」三层，提供：

- 面向设备舰队的**版本化部署下发**：重复下发幂等、非法版本/能力不兼容拒绝（原因可区分）、可复现回滚到任一历史版本；
- 边缘侧**断网缓冲 + 按位点续传**：重复上报、乱序到达、进程重启均不丢数据、不重复生效，状态冲突按内容确定性收敛；
- **多集群多租户并发**：跨租户访问与超配额请求被拒绝（原因可区分），并发更新线性化、读状态始终确定。

## 1. 目录结构

```
pkg/fleet/   共享领域类型：身份、版本、能力、部署规格、遥测/状态条目、拒绝原因枚举
pkg/cloud/   模拟云端控制面：租户/设备目录、配额、设备影子、Ingest 入口
pkg/deploy/  版本化部署管理：校验、幂等下发、不可变版本历史、回滚
pkg/edge/    模拟边缘网关：崩溃安全的本地日志(store.go)、按位点续传(gateway.go)、
             可注入断网/抖动的模拟链路(link.go)
```

## 2. 部署版本与回滚语义

### 2.1 版本

版本为 `MAJOR.MINOR.PATCH` 整数三元组（`fleet.Version`），按**数值**比较，
因此 `1.10.0 > 1.9.0`。`cloud.ParseVersion` 只接受恰好三段、非负整数，
其余一律 `invalid_version`（如 `1.2`、`1.2.x`、`v1.2.3`、空串）。

### 2.2 每台设备一条只增历史

`deploy.Manager` 为每台 `(租户, 设备)` 维护一条只增的 `Revision` 日志：

| 记录状态 | 含义 |
|---|---|
| `applied` | 规格校验通过并成为当前生效版本 |
| `rejected` | 下发被拒（审计证据，**绝不改变 Current**） |
| `rolled_back` | 回滚标记，携带被恢复的历史 `applied` 记录的**完整规格**，`RollbackTo` 指向其 Seq |

Seq 是从 1 开始、无空洞的整数。设备先经云端目录鉴权（见第 4 节），
之后：

1. 结构校验：版本非 0.0.0、各分量非负、payload 非空 → 否则 `invalid_version`；
2. 兼容性校验：
   - 设备平台 `Capabilities.MinPlatform < spec.MinPlatform`；
   - 设备缺少 `spec.RequiresFeatures` 中任一特性；
   - 两者均为 `incompatible_device`，detail 中说明具体是平台还是特性列表；
3. 配额：仅当一条记录**真正生效**时才原子占用一个 rollout-record 配额
   （`cloud.ReserveRollout`）；拒绝记录与幂等 no-op 不占配额。

### 2.3 幂等下发

对相同目标重复下发**完全相同的规格**（特性列表按集合比较，顺序不同视为相同）：

- 返回**原来那条记录**（相同 Seq），不新增记录、不占配额；
- 原记录是 `rejected` 时，重复下发复现同一个拒绝（相同 `Reason`）。

不同版本（即使 Payload 相同）才会追加新的 `applied` 记录并移动 Current。

### 2.4 回滚

`Rollback(tenant, device, seq)`：

- `seq` 必须存在（否则 `revision_not_found`），且必须是历史 `applied`
  记录（回滚标记/拒绝记录不可作为回滚目标 → `invalid_version`）；
- 在历史末尾追加一条 `rolled_back` 记录，其 Spec 与目标记录**逐字段相同**，
  因此仅凭历史日志即可复现任意时刻的生效版本；
- 当目标规格已是当前生效版本时，重复回滚是**幂等 no-op**（返回当前记录）；
- 回滚同样占用一个 rollout-record 配额（它是一次新生效变更）。

## 3. 同步与冲突处理规则

### 3.1 边缘本地日志（断网缓冲）

`edge.Store` 在网关目录维护两个文件：

- `log.jsonl`：**只追加**的条目日志（一行一条 JSON，带单调 Seq）。
  `Append` 在返回成功前 `write + fsync`；进程崩溃产生的半行尾部在重新打开时
  被识别并截断（已落盘前缀永不丢失）。
- `meta.json`：**位点（checkpoint）+ NextSeq + 已见 EventID 集合**，
  每次推进位点都用「临时文件 + fsync + rename + 目录 fsync」原子替换。

不变式：

- 条目只有在 fsync 进 `log.jsonl` 后才向生产者确认 → 崩溃不丢未确认数据；
- 位点只有在云端已处理（applied 或幂等 duplicate）后才推进 → 不会跳过未处理条目；
- 重开 Store 时重放日志重建内存状态与 seen 集合，`Pending()` 返回
  **Seq 严格大于 checkpoint** 的条目 —— 这就是重连/重启后的续传起点。

### 3.2 去重（生产者重试不生效两次）

条目以生产者分配的 `EventID` 全局去重，去重在两层独立成立：

1. **网关本地**：`Store.Append` 对重复 EventID 直接返回 `duplicate_id`，
   网络根本不参与（在线/离线、重启后都成立，seen 集合从日志重建）。
2. **云端**：`ControlPlane.Ingest` 对已应用的 EventID 返回
   `Ack{Duplicate:true}`，不改变任何状态、不增加 Rev。

因此传输层可以是 at-least-once（典型：云端已处理但应答丢失，网关重启后重发），
端到端效果仍是 exactly-once（见 `TestRestartDuringTransientRetry`）。

### 3.3 按位点续传与乱序

`Gateway.Flush` 严格按日志 Seq 顺序发送：每条收到云端确认后先
`Commit(seq)` 再发下一条；遇到**瞬时故障**（`edge.ErrTransient` 或链路 down）
立即在该条停止，下次 Flush 从同一位置继续；云端的**永久拒绝**
（`fleet.RejectError`，如设备已注销）不可通过重试改变，会提交跳过以免阻塞队列，
并把结构化原因返回调用方。

- 遥测：按到达顺序只增保存，EventID 去重，永不重排、不覆盖。
- 状态：云端影子按下面的确定性规则做字段级合并，因此**乱序到达收敛到同一结果**。

### 3.4 确定性冲突收敛（不静默覆盖）

对同一状态 key 的并发/乱序写入，胜出规则只依赖条目内容，与到达顺序无关：

1. `BaseRev`（该写入所基于的影子版本号）**更大者胜**；
2. BaseRev 相同 → `EventID` **字典序更小者胜**。

- 新 key 直接写入；
- 败方条目的值**不落地**，但该 key 会出现在 `Ack.Conflict` 中显式回报，
  而不是被静默丢弃；
- 影子 `Rev` = 已处理的**不同 EventID 条目数**（冲突败方也计数，重复条目不计数），
  所以同一组条目以任意排列到达，字段值与 Rev 都相同（`TestOutOfOrderStateConvergence`
  用正序/逆序断言了这一点）。

## 4. 配额与权限边界

- 身份：`租户 → 集群 → 设备`。所有设备相关操作（Lookup / Ingest / Shadow /
  Dispatch / Rollback / History）都先经目录鉴权。
- 拒绝原因互不相同时：

| 场景 | Reason |
|---|---|
| 调用方租户未注册 | `unknown_tenant` |
| 设备在本租户不存在且全局无此 ID | `unknown_device` |
| 设备存在但属于**另一个租户**（含用他人设备 ID 注册） | `cross_tenant` |
| 部署规格非法（版本/payload） | `invalid_version` |
| 平台过低或缺特性 | `incompatible_device` |
| 回滚目标 Seq 不存在 | `revision_not_found` |
| EventID 重复（本地或云端） | `duplicate_id` |
| 设备数 / rollout 记录数超租户配额 | `quota_exceeded` |

- 配额按租户独立计数，互不透支（一个租户耗尽不影响另一个）。
- 并发：云端与 deploy.Manager 各用单把互斥锁串行化所有判定，所有读返回深拷贝快照。
  任意时刻读到的都是某个已完成操作的一致状态（线性化）；同一资源的并发更新
  不可能交错破坏历史（`TestConcurrentDispatchNoCorruption`、
  `TestConcurrentStateUpdates` 在 `-race` 下验证）。

## 5. 本地验证方法

要求 Go 1.25+（开发使用 1.26）。

```bash
# 编译 + 静态检查
go build ./...
go vet ./...

# 全量测试（竞态检测）；测试日志会打印每条输入与判定依据
go test ./... -race -v

# 稳定性复现（结论必须每次相同）
go test ./... -race -count=10
go test ./... -count=200
```

### 关键测试与覆盖点

| 测试 | 验证内容 |
|---|---|
| `cloud.TestTenantIsolation` | 未知租户/设备、跨租户读写与注册，原因逐一可区分 |
| `cloud.TestDeviceQuota` | 设备配额耗尽 = `quota_exceeded`，且不跨租户泄漏 |
| `cloud.TestOutOfOrderStateConvergence` | 同一组状态写入正序/逆序到达，影子字段与 Rev 完全相同 |
| `cloud.TestTelemetryDedupAndDuplicateAck` | 遥测只生效一次、重复返回 Duplicate、顺序保持 |
| `cloud.TestConcurrentStateUpdates` | 100 个并发写不丢更新，同 key 竞争有确定赢家，并发读无撕裂 |
| `deploy.TestDispatchIdempotent` | 重复下发同 Seq、不占配额；特性顺序不同仍判同 |
| `deploy.TestDispatchValidation` | 非法版本/平台过低/缺特性/跨租户/未知设备原因可区分；拒绝不改 Current |
| `deploy.TestRollbackReproducible` | 回滚恢复完整规格、幂等、非法目标原因可区分、历史可重放 |
| `deploy.TestRolloutQuota` | 仅生效记录占配额；耗尽后幂等重试仍成功、回滚被配额拒绝 |
| `deploy.TestConcurrentDispatchNoCorruption` | 40 个并发下发线性化，每版本恰好 applied 一次，Seq 无空洞 |
| `edge.TestOfflineBufferAndResume` | 断网缓冲、离线 Flush 被拒、重连按位点一次发完、再次重连零发送 |
| `edge.TestProducerDuplicateRejected` | 同 EventID 重试在本地被 `duplicate_id` 拒绝，不触网 |
| `edge.TestTransientFailureResumesAtCheckpoint` | 抖动失败不推进位点，重试从断点继续，条目恰好一次 |
| `edge.TestRestartRecovery` | 进程重启后重放日志：已提交的不重发、未提交的不丢失 |
| `edge.TestRestartDuringTransientRetry` | 应答丢失 + 重启重发，云端去重保证 exactly-once 效果 |
| `edge.TestTornLogTailTruncation` | 崩溃写坏的日志尾被截断，位点/已见集合正确恢复 |
| `edge.TestConcurrentProducers` | 80 个并发生产（每事件 2 份）恰好 40 条生效、40 次重复拒绝 |
| `edge.TestEndToEndDeployAndSync` | 云+部署+网关端到端：下发、断网期间回滚、重连后冲突确定性收敛 |

所有测试均使用 `t.Logf` 打印**输入与判定依据**（结构化 reason、checkpoint、
attempt 日志、最终影子），`go test -v` 即可看到完整决策轨迹。
