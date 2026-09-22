# edge-fleet-orchestrator-go

用本地内存与本地文件模拟“云端控制面 — 边缘网关 — 设备端”的边缘设备舰队编排系统，
覆盖版本化部署下发、断网缓冲与位点续传、状态回流与冲突收敛、多租户/多集群的配额与权限边界。

## 结构

| 包 | 角色 |
|---|---|
| `fleet` | 共享领域类型：`Artifact`/`DeploymentSpec`/`Telemetry`/`Checkpoint`，以及可区分失败原因 `Reason` |
| `controlplane` | 云端控制面（内存实现，互斥锁保证并发确定）：部署、幂等、回滚、配额、遥测汇聚 |
| `edge` | 边缘侧：`Gateway`（断网缓冲/重连续传）、`Store`（JSON 落盘 + 原子改名）、`Device`（设备端代理） |

## 部署版本与回滚语义

- **版本格式**：制品版本必须匹配 `vMAJOR.MINOR.PATCH`，否则拒绝并返回 `INVALID_VERSION`。
- **兼容性校验**：制品的 OS/架构、大小上限、所需特性必须与设备 `Capabilities` 匹配，否则返回 `INCOMPATIBLE`。
- **幂等下发**：`DeploymentSpec.IdempotencyKey` 相同的重复下发直接返回首次的 `DeploymentRecord`
  （同一 `Seq`），不产生新的历史记录，不消耗额外效果。
- **历史与回滚**：每次成功部署追加一条 `DeploymentRecord`（全局单调 `Seq`）。
  `Rollback(tenant, device, version)` 在历史中查找目标版本，**复用当时记录的完整 Spec** 生成新记录
  （`RollbackOf` 标记目标版本），因此对同一目标的回滚是可复现的；目标版本不在历史中返回 `NOT_FOUND`。

## 同步与冲突处理规则

- **位点（Seq）**：设备端 `Emit` 产生单调递增位点；网关 `Report` 先写本地 `Store` 再尝试上传，
  断网期间数据只留在本地缓冲，不会丢失。
- **续传**：重连后 `Sync` 从本地 `Checkpoint`（已确认的最大连续位点）之后开始上传；
  控制面确认后网关 `Ack` 推进位点并清理已确认缓冲。
- **去重**：控制面按 `(DeviceID, Seq)` 去重，重复上报不重复生效，且重复批次仍返回当前确认位点。
- **乱序容忍**：位点可乱序到达；确认位点只按“连续”推进。
- **冲突收敛**：同一 `(DeviceID, Key)` 的状态按**位点大者胜**收敛；位点相同保留先到值。
  收敛结果与到达顺序无关（确定性），不会静默覆盖——任何时刻 `State` 读到的都是该规则下的确定值。
- **重启恢复**：`Store` 每次变更原子落盘（临时文件 + rename）；进程重启后用同一路径
  `OpenStore` 即可恢复缓冲与位点，续传不丢不重。

## 配额与权限边界

- **租户隔离**：设备注册时建立全局 `deviceID -> tenantID` 索引；跨租户访问设备返回 `FORBIDDEN`，
  与资源不存在（`NOT_FOUND`）可区分。
- **配额**：`Quota{MaxDevices, MaxDeployments}` 按租户计；超限返回 `QUOTA_EXCEEDED`。
- **失败原因**：所有领域错误携带 `fleet.Reason`，用 `fleet.ReasonOf(err)` 提取，取值：
  `INVALID_VERSION` / `INCOMPATIBLE` / `NOT_FOUND` / `FORBIDDEN` / `QUOTA_EXCEEDED` 等。
- **并发确定性**：控制面全部状态变更在单互斥锁下完成；同一资源的并发更新被串行化，
  任意时刻读取到的状态都是确定的。

## 本地验证

```bash
go build ./...
go test ./...            # 单测日志打印输入与判定依据（go test -v 可见）
go test -race -count=5 ./...   # 竞态检测 + 重复运行验证结论稳定
```

测试覆盖：部署幂等/非法版本/能力不兼容/可复现回滚、断网缓冲与重连续传、
重复与乱序上报、冲突确定性收敛、进程重启恢复、并发上报一致性、跨租户拒绝与配额超限。
