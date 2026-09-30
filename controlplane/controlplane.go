// Package controlplane 模拟云端控制面：多租户/多集群的部署下发、
// 幂等去重、配额与权限校验、回滚以及遥测汇聚。
package controlplane

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// Quota 是租户级配额。
type Quota struct {
	MaxDevices     int
	MaxDeployments int
}

// stateEntry 是某状态键当前收敛出的获胜上报（收敛只依赖其 Seq/Value）。
type stateEntry struct {
	win fleet.Telemetry
}

type deviceState struct {
	caps           fleet.Capabilities
	clusterID      string
	history        []fleet.DeploymentRecord
	currentVersion string
	contentHash    string                             // 当前生效内容的规范化指纹
	contentFirst   map[string]*fleet.DeploymentRecord // 每个内容指纹第一次生效的记录
	idempotency    map[string]*fleet.DeploymentRecord
	receivedSeq    map[uint64]bool // 已接收的遥测位点（用于连续确认位点推进）
	seenTelemetry  map[string]bool // 已接收的 (Key,Seq,Value) 去重索引
	ackedSeq       uint64          // 已确认的最大连续位点
	state          map[string]stateEntry
	conflicts      []fleet.ConflictReport // 该设备累积的冲突淘汰记录
	reportDedup    map[string]bool        // 冲突报告去重索引（重放不重复记录）
}

type tenantState struct {
	quota       Quota
	deployCount int
	devices     map[string]*deviceState // "clusterID\x00deviceID" -> state
}

// ControlPlane 是云端控制面的内存实现，并发安全。
type ControlPlane struct {
	mu        sync.Mutex
	tenants   map[string]*tenantState
	deviceIdx map[string]map[string]string // deviceID -> (tenantID -> clusterID)，用于区分跨租户/跨集群与不存在
	nextSeq   int64
}

// New 创建空控制面。
func New() *ControlPlane {
	return &ControlPlane{
		tenants:   make(map[string]*tenantState),
		deviceIdx: make(map[string]map[string]string),
	}
}

// deviceKey 是租户内的设备复合键：集群 + 设备。
func deviceKey(clusterID, deviceID string) string { return clusterID + "\x00" + deviceID }

// SetQuota 设置租户配额（首次调用隐式创建租户）。
func (c *ControlPlane) SetQuota(tenantID string, q Quota) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tenants[tenantID]
	if t == nil {
		t = &tenantState{devices: make(map[string]*deviceState)}
		c.tenants[tenantID] = t
	}
	t.quota = q
}

// RegisterDevice 注册设备到租户与集群。设备的有效标识是 (租户, 集群, 设备ID)：
// 同租户不同集群下的同名设备相互独立；不同租户复用同名设备一律拒绝。
func (c *ControlPlane) RegisterDevice(tenantID, clusterID, deviceID string, caps fleet.Capabilities) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tenants[tenantID]
	if t == nil {
		t = &tenantState{devices: make(map[string]*deviceState)}
		c.tenants[tenantID] = t
	}
	// 跨租户：该 deviceID 已被其他租户注册。
	if owners := c.deviceIdx[deviceID]; owners != nil {
		if owner := owners[tenantID]; owner == "" {
			for other := range owners {
				return fleet.NewForbidden("device %q belongs to tenant %q, not %q", deviceID, other, tenantID)
			}
		}
	}
	key := deviceKey(clusterID, deviceID)
	if _, ok := t.devices[key]; ok {
		return nil // 重复注册幂等
	}
	if t.quota.MaxDevices > 0 && len(t.devices) >= t.quota.MaxDevices {
		return fleet.NewQuotaExceeded("tenant %q device quota %d exceeded", tenantID, t.quota.MaxDevices)
	}
	t.devices[key] = &deviceState{
		caps:          caps,
		clusterID:     clusterID,
		contentFirst:  make(map[string]*fleet.DeploymentRecord),
		idempotency:   make(map[string]*fleet.DeploymentRecord),
		receivedSeq:   make(map[uint64]bool),
		seenTelemetry: make(map[string]bool),
		state:         make(map[string]stateEntry),
		reportDedup:   make(map[string]bool),
	}
	if c.deviceIdx[deviceID] == nil {
		c.deviceIdx[deviceID] = make(map[string]string)
	}
	c.deviceIdx[deviceID][tenantID] = clusterID
	return nil
}

var versionRe = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

// lookup 在持锁状态下定位设备。
// clusterID 为空且该租户内存在唯一匹配设备时回退到唯一集群；
// 跨租户访问返回 FORBIDDEN（与 NOT_FOUND 明确区分），其余找不到返回 NOT_FOUND。
func (c *ControlPlane) lookup(tenantID, clusterID, deviceID string) (*tenantState, *deviceState, error) {
	t := c.tenants[tenantID]
	if t == nil {
		if owners := c.deviceIdx[deviceID]; owners != nil && owners[tenantID] == "" {
			for other := range owners {
				return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, other)
			}
		}
		return nil, nil, fleet.NewNotFound("tenant %q not found", tenantID)
	}
	if clusterID != "" {
		d := t.devices[deviceKey(clusterID, deviceID)]
		if d != nil {
			return t, d, nil
		}
		if owners := c.deviceIdx[deviceID]; owners != nil && owners[tenantID] == "" {
			for other := range owners {
				return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, other)
			}
		}
		return nil, nil, fleet.NewNotFound("device %q not found in tenant %q cluster %q", deviceID, tenantID, clusterID)
	}
	// clusterID 缺省：跨设备唯一标识仍由 deviceID 保证，查找唯一集群。
	var found *deviceState
	for k, d := range t.devices {
		if keyDeviceID(k) == deviceID {
			if found != nil {
				return nil, nil, fleet.NewNotFound("device %q is ambiguous in tenant %q across clusters; specify cluster", deviceID, tenantID)
			}
			found = d
		}
	}
	if found != nil {
		return t, found, nil
	}
	if owners := c.deviceIdx[deviceID]; owners != nil && owners[tenantID] == "" {
		for other := range owners {
			return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, other)
		}
	}
	return nil, nil, fleet.NewNotFound("device %q not found in tenant %q", deviceID, tenantID)
}

// keyDeviceID 从复合键中取出 deviceID（复合键形如 clusterID\x00deviceID）。
func keyDeviceID(key string) string {
	if i := strings.IndexByte(key, 0); i >= 0 {
		return key[i+1:]
	}
	return key
}

// contentFingerprint 返回下发内容（版本 + 制品属性 + 依赖特性，特性按名排序后）的规范化指纹。
// 不包含 TenantID/ClusterID/DeviceID/IdempotencyKey：内容相同即视为同一内容，
// 但生效范围由调用处的 (租户, 集群, 设备) 限定，互不顶掉。
func contentFingerprint(a fleet.Artifact) string {
	feats := append([]string(nil), a.RequireFeature...)
	sort.Strings(feats)
	return fmt.Sprintf("v=%s|os=%s|arch=%s|size=%d|feat=%s",
		a.Version, a.OS, a.Arch, a.SizeBytes, strings.Join(feats, ","))
}

// Deploy 版本化部署下发；幂等、校验版本与能力、配额控制。
//
// 幂等分两层：
//  1. 显式幂等键：同键重复返回首次结果（历史行为保留）。
//  2. 内容级幂等：在同一 (租户, 集群, 设备) 上，只要下发内容与此前生效过的某条完全一致
//     （版本/OS/架构/大小/依赖特性均未变），无论是否带标识、标识是否变化，都直接返回
//     该内容第一次生效的记录：不追加历史、不推进版本、不消耗部署额度。
//     只有目标（不同集群/不同设备）或内容真正变化才算一次新部署。
//
// 校验顺序保证被拒请求无副作用：先做版本/能力校验，再判内容幂等，最后才查配额并落库。
func (c *ControlPlane) Deploy(spec fleet.DeploymentSpec) (*fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, d, err := c.lookup(spec.TenantID, spec.ClusterID, spec.DeviceID)
	if err != nil {
		return nil, err
	}
	// 层 1：显式幂等键直接返回首次结果。
	if spec.IdempotencyKey != "" {
		if rec, ok := d.idempotency[spec.IdempotencyKey]; ok {
			cp := *rec
			return &cp, nil
		}
	}
	// 版本格式校验。
	if !versionRe.MatchString(spec.Artifact.Version) {
		return nil, fleet.NewInvalidVersion("version %q must match vMAJOR.MINOR.PATCH", spec.Artifact.Version)
	}
	// 能力兼容性校验。
	if err := checkCompatible(d.caps, spec.Artifact); err != nil {
		return nil, err
	}
	// 层 2：内容级幂等。该内容只要在本设备上生效过（包括回滚后再次下发历史内容），
	// 就返回该内容第一次生效的记录：不追加历史、不改当前版本、不消耗额度；
	// 配额已满时同样允许重复生效。
	fp := contentFingerprint(spec.Artifact)
	if first, ok := d.contentFirst[fp]; ok {
		cp := *first
		return &cp, nil
	}
	// 配额校验（只有真正新增的一次部署才走到这里）。
	if t.quota.MaxDeployments > 0 && t.deployCount >= t.quota.MaxDeployments {
		return nil, fleet.NewQuotaExceeded("tenant %q deployment quota %d exceeded", spec.TenantID, t.quota.MaxDeployments)
	}
	rec := c.appendRecord(t, d, spec, "", fp)
	if spec.IdempotencyKey != "" {
		d.idempotency[spec.IdempotencyKey] = rec
	}
	cp := *rec
	return &cp, nil
}

func checkCompatible(caps fleet.Capabilities, a fleet.Artifact) error {
	if a.Arch != caps.Arch || a.OS != caps.OS {
		return fleet.NewIncompatible("artifact %s/%s incompatible with device %s/%s", a.OS, a.Arch, caps.OS, caps.Arch)
	}
	if caps.MaxArtifact > 0 && a.SizeBytes > caps.MaxArtifact {
		return fleet.NewIncompatible("artifact size %d exceeds device limit %d", a.SizeBytes, caps.MaxArtifact)
	}
	for _, f := range a.RequireFeature {
		if !caps.Supports(f) {
			return fleet.NewIncompatible("device lacks required feature %q", f)
		}
	}
	return nil
}

func (c *ControlPlane) appendRecord(t *tenantState, d *deviceState, spec fleet.DeploymentSpec, rollbackOf, fp string) *fleet.DeploymentRecord {
	c.nextSeq++
	rec := fleet.DeploymentRecord{
		Seq:        c.nextSeq,
		Spec:       spec,
		RollbackOf: rollbackOf,
		AppliedAt:  time.Now().UTC(),
	}
	d.history = append(d.history, rec)
	d.currentVersion = spec.Artifact.Version
	d.contentHash = fp
	if d.contentFirst == nil {
		d.contentFirst = make(map[string]*fleet.DeploymentRecord)
	}
	if _, ok := d.contentFirst[fp]; !ok {
		d.contentFirst[fp] = &d.history[len(d.history)-1]
	}
	t.deployCount++
	return &d.history[len(d.history)-1]
}

// Rollback 将设备可复现地恢复到指定历史版本：复用历史中记录的完整 Spec。
//
// 幂等规则与部署一致：
//   - 当前已是目标版本时（对同一版本重复触发回滚，或目标与当前内容相同），
//     直接返回该内容第一次生效的记录，不追加历史、不消耗额度；
//   - 目标版本从未在历史中出现返回 NOT_FOUND；
//   - 只有真正把当前版本切换到目标版本的第一次回滚才追加一条 RollbackOf 记录并计额度。
func (c *ControlPlane) Rollback(tenantID, deviceID, targetVersion string) (*fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, d, err := c.lookup(tenantID, "", deviceID)
	if err != nil {
		return nil, err
	}
	var target *fleet.DeploymentRecord
	for i := range d.history {
		if d.history[i].Spec.Artifact.Version == targetVersion {
			target = &d.history[i]
		}
	}
	if target == nil {
		return nil, fleet.NewNotFound("device %q has no historical version %q", deviceID, targetVersion)
	}
	// 可复现：使用历史记录中的原始 Spec，剥离幂等键（内容级幂等不再依赖它）。
	spec := target.Spec
	spec.IdempotencyKey = ""
	fp := contentFingerprint(spec.Artifact)
	// 幂等：当前生效内容就是目标内容，直接返回该内容第一次生效的记录。
	if d.contentHash == fp {
		if first, ok := d.contentFirst[fp]; ok {
			cp := *first
			return &cp, nil
		}
	}
	if t.quota.MaxDeployments > 0 && t.deployCount >= t.quota.MaxDeployments {
		return nil, fleet.NewQuotaExceeded("tenant %q deployment quota %d exceeded", tenantID, t.quota.MaxDeployments)
	}
	rec := c.appendRecord(t, d, spec, targetVersion, fp)
	cp := *rec
	return &cp, nil
}

// conflictDedupKey 让同一条冲突在反复重放/并发下只记录一次。
func conflictDedupKey(r fleet.ConflictReport) string {
	return fmt.Sprintf("%s\x00%d\x00%s\x00%s", r.Key, r.Seq, r.Winner.Value, r.Loser.Value)
}

// converge 是纯函数：给定同一位置的当前获胜值与新到值，只依据内容 (Seq, Value) 决定胜负。
// 规则：位点大者胜；位点相同则取值字典序较大者（与来源、到达顺序、批次切分均无关）。
// 返回 winner、loser（若发生同位点不同值冲突）以及是否构成“冲突淘汰”。
func converge(cur, in fleet.Telemetry) (winner fleet.Telemetry, loser fleet.Telemetry, conflict bool) {
	switch {
	case in.Seq > cur.Seq:
		return in, fleet.Telemetry{}, false // 正常版本推进：旧值过期，不算冲突
	case in.Seq < cur.Seq:
		return cur, fleet.Telemetry{}, false // 过期值被丢弃，按位点规则收敛，不算冲突
	default:
		if in.Value == cur.Value {
			return cur, fleet.Telemetry{}, false // 同内容重复，去重层处理
		}
		if in.Value > cur.Value {
			return in, cur, true // 同位点：值字典序较大者胜，当前值被淘汰
		}
		return cur, in, true // 同位点：新到值被淘汰
	}
}

// applyOne 在持锁状态下处理一条上报，返回该设备最新确认位点与可能产生的冲突报告。
// 完全相同的 (Key,Seq,Value) 重复到达直接跳过（去重，不算冲突、不重复记录）。
func (c *ControlPlane) applyOne(d *deviceState, rec fleet.Telemetry) (uint64, *fleet.ConflictReport) {
	tid := telemetryID(rec)
	if d.seenTelemetry[tid] {
		return d.ackedSeq, nil
	}
	d.seenTelemetry[tid] = true
	d.receivedSeq[rec.Seq] = true

	var report *fleet.ConflictReport
	cur, exists := d.state[rec.Key]
	if !exists {
		d.state[rec.Key] = stateEntry{win: rec}
	} else {
		winner, loser, isConflict := converge(cur.win, rec)
		d.state[rec.Key] = stateEntry{win: winner}
		if isConflict {
			report = &fleet.ConflictReport{
				ClusterID: d.clusterID,
				DeviceID:  rec.DeviceID,
				Key:       rec.Key,
				Seq:       rec.Seq,
				Winner:    winner,
				Loser:     loser,
				Basis: fmt.Sprintf(
					"同一位置 %q 位点 %d 出现不同值 %q/%q；按 (Seq, Value) 字典序取较大值 %q，%q 淘汰（仅依据上报内容，与到达顺序/批次/并发无关）",
					rec.Key, rec.Seq, winner.Value, loser.Value, winner.Value, loser.Value),
				Reason: fleet.ReasonConflictResolved,
				At:     time.Now().UTC(),
			}
		}
	}
	// 推进连续确认位点。
	for d.receivedSeq[d.ackedSeq+1] {
		d.ackedSeq++
	}
	if report != nil {
		key := conflictDedupKey(*report)
		if !d.reportDedup[key] {
			d.reportDedup[key] = true
			d.conflicts = append(d.conflicts, *report)
		} else {
			report = nil
		}
	}
	return d.ackedSeq, report
}

// telemetryID 是一条上报内容的唯一标识（位置 + 位点 + 值）。
func telemetryID(r fleet.Telemetry) string {
	return fmt.Sprintf("%s\x00%d\x00%s", r.Key, r.Seq, r.Value)
}

// IngestDetailed 接收网关重连后续传的遥测；按内容去重、乱序容忍、确定性冲突收敛，
// 并回报本次处理中确认的冲突淘汰记录（重放相同内容不会重复产生报告）。
func (c *ControlPlane) IngestDetailed(tenantID string, batch []fleet.Telemetry) (fleet.Checkpoint, []fleet.ConflictReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 两阶段：先解析全部设备归属，任一不合法则整批不产生副作用。
	resolved := make([]*deviceState, len(batch))
	for i, rec := range batch {
		_, d, err := c.lookup(tenantID, rec.ClusterID, rec.DeviceID)
		if err != nil {
			return fleet.Checkpoint{}, nil, err
		}
		resolved[i] = d
	}
	var cp fleet.Checkpoint
	var reports []fleet.ConflictReport
	for i, rec := range batch {
		acked, report := c.applyOne(resolved[i], rec)
		if report != nil {
			reports = append(reports, *report)
		}
		cp = fleet.Checkpoint{DeviceID: rec.DeviceID, AckedSeq: acked}
	}
	// 冲突报告集合只由内容决定：按位置/位点/值排序，保证多次运行输出顺序也稳定。
	sort.Slice(reports, func(i, j int) bool {
		return conflictDedupKey(reports[i]) < conflictDedupKey(reports[j])
	})
	return cp, reports, nil
}

// Ingest 接收网关重连后续传的遥测；按位点去重、乱序容忍、冲突确定性收敛。
// 收敛规则：同一 (device,key) 按 (Seq, Value) 字典序收敛——位点大者胜；
// 位点相同取值字典序较大者。收敛结果与到达顺序、批次切分、并发路数无关，
// 同位点不同值的被淘汰方可通过 Conflicts 查询其淘汰原因。
func (c *ControlPlane) Ingest(tenantID string, batch []fleet.Telemetry) (fleet.Checkpoint, error) {
	cp, _, err := c.IngestDetailed(tenantID, batch)
	return cp, err
}

// State 读取设备某状态键的确定值。
func (c *ControlPlane) State(tenantID, clusterID, deviceID, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return "", false, err
	}
	e, ok := d.state[key]
	return e.win.Value, ok, nil
}

// Conflicts 返回设备累积的冲突淘汰记录（按记录顺序），用于解释最终值的由来。
func (c *ControlPlane) Conflicts(tenantID, clusterID, deviceID string) ([]fleet.ConflictReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return nil, err
	}
	out := make([]fleet.ConflictReport, len(d.conflicts))
	copy(out, d.conflicts)
	return out, nil
}

// History 返回设备的部署历史（按 Seq 升序）。
func (c *ControlPlane) History(tenantID, clusterID, deviceID string) ([]fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return nil, err
	}
	out := make([]fleet.DeploymentRecord, len(d.history))
	copy(out, d.history)
	return out, nil
}

// CurrentVersion 返回设备当前部署版本。
func (c *ControlPlane) CurrentVersion(tenantID, clusterID, deviceID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return "", err
	}
	return d.currentVersion, nil
}

// DeployCount 返回租户已生效的部署计数（重复内容不计入），供观测与测试使用。
func (c *ControlPlane) DeployCount(tenantID string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tenants[tenantID]
	if t == nil {
		return 0, fleet.NewNotFound("tenant %q not found", tenantID)
	}
	return t.deployCount, nil
}
